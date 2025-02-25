import Redis, { RedisOptions } from 'ioredis';
import { createPool, Pool } from 'generic-pool';
import { Logger } from '../utils/logger';
import { createRedisPool } from '../utils/redisPool';
import { LuaScriptLoader } from '../utils/LuaScriptLoader';
import { IJobData } from '../interfaces/queue.interface';

export class Queue {
    private client: Redis;
    private pool: Pool<Redis>;
    private logger: Logger;
    private scriptLoader: LuaScriptLoader;
    private scriptShas: Map<string, string> = new Map();

    private publisherClient: Redis;
    private subscriberClient: Redis;

    private processMap = new Map();

    constructor(private credentials: RedisOptions, logLevel: string = 'debug') {
        this.logger = new Logger(logLevel);
        this.client = new Redis(credentials);
        this.pool = createRedisPool(credentials);
        this.scriptLoader = new LuaScriptLoader("lua-scripts");

        this.publisherClient = new Redis(credentials);
        this.subscriberClient = new Redis(credentials);

        this.setupSubscriber();
    }

    public async init(): Promise<void> {
        this.logger.info('Inicializando la cola...');
        await this.loadAndRegisterScripts(['enqueue', 'dequeue', 'update_status', 'get_status', 'limit_consumers']);

        this.startMonitor()

    }

    private async loadAndRegisterScripts(scriptNames: string[]): Promise<void> {
        for (const name of scriptNames) {
            await this.scriptLoader.loadScript(name);
            const scriptContent = this.scriptLoader.getScript(name);
            if (scriptContent) {
                const client = await this.pool.acquire();
                try {
                    const sha = await client.script('LOAD', scriptContent) as string;
                    this.scriptShas.set(name, sha);
                } finally {
                    this.pool.release(client);
                }
            } else {
                this.logger.error(`No se pudo cargar el script: ${name}`);
            }
        }
    }

    private async executeScript(scriptName: string, keys: string[] = [], args: (string | number)[] = []): Promise<any> {
        const sha = this.scriptShas.get(scriptName);
        if (!sha) {
            throw new Error(`Script ${scriptName} no está registrado.`);
        }
        return this.client.evalsha(sha, keys.length, ...keys, ...args);
    }

    public async add(queueName: string, groupName: string, data: any): Promise<void> {
        const keys = [`qube:${queueName}:groups`, `qube:${queueName}:group:${groupName}`];
        const args = [JSON.stringify(data), groupName];

        const jobId = await this.executeScript('enqueue', keys, args);

        this.logger.info(`jobId: ${jobId}`);

        await this.publisherClient.publish(`QUEUE:NEWJOB`, JSON.stringify({ queueName, groupName }));

        return jobId;
    }

    public async process(queueName: string, nConsumers: number, callback: (job: IJobData, done: (err?: Error) => void) => void): Promise<void> {
        this.logger.info(`Registrando procesadores para la cola '${queueName}' con ${nConsumers} consumidores`);
        const client = await this.pool.acquire();
        try {
            this.processMap.set(queueName, { callback, nConsumers });
        } finally {
            this.pool.release(client);
        }
    }

    private async isConsumerBusy(queueName: string, groupName: string, workerId: string): Promise<boolean> {
        const client = await this.pool.acquire();
        try {
            const consumerKey = `qube:consumer:${queueName}:${groupName}:${workerId}`;
            const exists = await client.exists(consumerKey);
            return exists === 1;
        } finally {
            this.pool.release(client);
        }
    }

    private async getGroupNames(queueName: string): Promise<string[]> {
        const client = await this.pool.acquire();
        const pattern = `qube:${queueName}:group:*`;
        let cursor = '0';
        let groupNames: string[] = [];

        try {
            do {
                const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
                cursor = nextCursor;
                const groups = keys.map(key => key.split(':').pop()!); // Extrae el groupName del key
                groupNames.push(...groups);
            } while (cursor !== '0');

            return groupNames;
        } finally {
            this.pool.release(client);
        }
    }

    private async removeConsumer(queueName: string, groupName: string, workerId: string): Promise<void> {
        const client = await this.pool.acquire();
        try {
            const consumerKey = `qube:consumer:${queueName}:${groupName}:${workerId}`;
            await client.del(consumerKey);
            this.logger.info(`Consumidor eliminado: ${consumerKey}`);
        } finally {
            this.pool.release(client);
        }
    }

    private async startMonitor(): Promise<void> {
        const interval = 10 * 1000; // Cada 10 segundos

        setInterval(async () => {
            this.logger.info('Ejecutando tarea periódica cada 10 segundos');

            for (const [queueName, { nConsumers }] of this.processMap.entries()) {
                this.logger.info(`Revisando la cola: ${queueName} con ${nConsumers} consumidores`);

                const groupNames = await this.getGroupNames(queueName);

                for (const groupName of groupNames) {
                    const client = await this.pool.acquire();
                    try {
                        // Escanear los consumidores activos con el patrón de STRINGS
                        const pattern = `qube:consumer:${queueName}:${groupName}:*`;
                        let cursor = '0';

                        do {
                            const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
                            cursor = nextCursor;

                            for (const consumerKey of keys) {
                                const workerId = consumerKey.split(':').pop()!;
                                const isBusy = await this.isConsumerBusy(queueName, groupName, workerId);

                                if (isBusy) {
                                    await this.renewConsumerTTL(queueName, groupName, workerId, 30); // Renovar TTL
                                    this.logger.info(`Renovado TTL para consumidor: ${consumerKey}`);
                                } else {
                                    await this.removeConsumer(queueName, groupName, workerId); // Eliminar consumidor
                                    this.logger.info(`Consumidor eliminado: ${consumerKey}`);
                                }
                            }
                        } while (cursor !== '0');
                    } finally {
                        this.pool.release(client);
                    }
                }
            }
        }, interval);
    }

    public async startGroupConsumer(queueName: string, groupName: string, workerId: string): Promise<void> {
        const client = await this.pool.acquire();
        try {
            this.logger.info(`START GROUP CONSUMER queueName: ${queueName}, groupName: ${groupName}, workerId: ${workerId}`);
            await this.groupWorker(queueName, groupName, workerId);
        } finally {
            this.pool.release(client);
        }
    }

    private async updateJobStatus(jobId: string, newStatus: string) {
        await this.executeScript('update_status', [], [jobId, newStatus]);
    }

    private async updateProgress(jobId: string, value: number): Promise<void> {
        const client = await this.pool.acquire();
        try {
            await client.hset(`qube:queue:job:${jobId}`, 'progress', value);
        } finally {
            this.pool.release(client);
        }
    }

    private async processJob(job: IJobData, fn: any) {
        const done = async (err?: Error) => {
            if (err) {
                await this.updateJobStatus(job.id, 'failed');
            } else {
                await job.progress(100);
                await this.updateJobStatus(job.id, 'completed');
            }
        };
        try {
            await fn(job, done);
        } catch (error) {
            await this.updateJobStatus(job.id, 'failed');
        }
    }

    private async canStartConsumer(queueName: string, groupName: string, maxConsumers: number): Promise<string | null> {
        const workerId = Math.random().toString(36).substr(2, 9);
        const keyPrefix = `qube:consumer:${queueName}:${groupName}`;
        const ttl = 30; // TTL en segundos
    
        const sha = this.scriptShas.get('limit_consumers');
        if (!sha) {
            throw new Error('El script limit_consumers no está cargado.');
        }
    
        const client = await this.pool.acquire();
        try {
            const result = await client.evalsha(
                sha,
                1, // Número de claves
                keyPrefix, // Prefijo de la clave
                maxConsumers,
                workerId,
                ttl
            );
    
            if (result === 1) {
                this.logger.info(`✅ Consumidor registrado con ID: ${workerId}`);
                return workerId; // Consumidor creado exitosamente
            } else {
                this.logger.info(`❌ No se pudo iniciar consumidor (máximo alcanzado)`);
                return null; // No se permitió crear el consumidor
            }
        } finally {
            this.pool.release(client);
        }
    }

    private async renewConsumerTTL(queueName: string, groupName: string, workerId: string, ttl: number): Promise<void> {
        const client = await this.pool.acquire();
        try {
            const consumerKey = `qube:consumer:${queueName}:${groupName}:${workerId}`;
            await client.expire(consumerKey, ttl);
            this.logger.info(`Renovado TTL para consumidor: ${consumerKey}`);
        } finally {
            this.pool.release(client);
        }
    }

    private async registerConsumer(queueName: string, groupName: string, workerId: string, ttl: number): Promise<void> {
        const client = await this.pool.acquire();
        try {
            const consumerKey = `qube:consumer:${queueName}:${groupName}:${workerId}`;
            await client.set(consumerKey, 'active', 'EX', ttl); // Crea un string con TTL
            this.logger.info(`Consumidor registrado: ${consumerKey}`);
        } finally {
            this.pool.release(client);
        }
    }

    // Uso en el método groupWorker para renovar el TTL mientras haya trabajos pendientes
    private async groupWorker(queueName: string, groupName: string, workerId: string): Promise<void> {
        const client = await this.pool.acquire();
        const ttl = 30; // TTL en segundos
        const refreshInterval = 10; // Renovar cada 10 segundos
    
        // Registrar el consumidor (usando el workerId recibido)
        await this.registerConsumer(queueName, groupName, workerId, ttl);
    
        const renewTTLInterval = setInterval(() => {
            this.renewConsumerTTL(queueName, groupName, workerId, ttl);
        }, refreshInterval * 1000);
    
        try {
            while (true) {
                const groupQueueKey = `qube:${queueName}:group:${groupName}`;
                const result = await this.executeScript('dequeue', [groupQueueKey]);
    
                if (result) {
                    const [jobId, jobDataRaw, groupNameFromJob] = result;
                    const jobDataParsed = jobDataRaw ? JSON.parse(jobDataRaw) : null;
                    const job: IJobData = {
                        id: jobId,
                        data: jobDataParsed,
                        groupName: groupNameFromJob ? groupNameFromJob.toString() : '',
                        progress: async (value: number) => await this.updateProgress(jobId, value)
                    };
                    await this.processJob(job, this.processMap.get(queueName).callback);
                } else {
                    break; // Salir cuando no haya trabajos pendientes
                }
            }
        } finally {
            clearInterval(renewTTLInterval);
            await this.removeConsumer(queueName, groupName, workerId);
            await this.pool.release(client);
        }
    }

    private setupSubscriber(): void {
        this.subscriberClient.on('message', async (channel: string, message: string) => {
            if (channel === 'QUEUE:NEWJOB') {
                const { queueName, groupName } = JSON.parse(message);
    
                if (this.processMap.has(queueName)) {
                    const nConsumers = this.processMap.get(queueName).nConsumers;
                    const workerId = await this.canStartConsumer(queueName, groupName, nConsumers);
                    if (!workerId) {
                        this.logger.info(`❌ Ya se alcanzó el máximo de consumidores para ${queueName}:${groupName}`);
                        return;
                    }
                    this.logger.info(`✅ Iniciando nuevo consumidor para ${queueName}:${groupName} con ID: ${workerId}`);
                    await this.startGroupConsumer(queueName, groupName, workerId);
                } else {
                    this.logger.warn(`⚠️ No hay callback asociado al queueName '${queueName}', ignorando notificación.`);
                }
            }
        });
    
        this.subscriberClient.subscribe('QUEUE:NEWJOB', (err, count) => {
            if (err) {
                this.logger.error(`Error suscribiéndose a 'QUEUE:NEWJOB': ${err.message}`);
            } else {
                this.logger.info(`🔔 Suscrito a ${count} canal(es), esperando mensajes...`);
            }
        });
    }

    public async close(): Promise<void> {
        this.logger.info('Cerrando conexiones...');
        await this.client.quit();
        await this.publisherClient.quit();
        await this.subscriberClient.quit();
        await this.pool.drain();
        await this.pool.clear();
    }
}