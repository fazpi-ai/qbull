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
    private groupConsumers = new Map<string, string[]>();

    private publisherClient: Redis;
    private subscriberClient: Redis;

    // processMap guarda la callback y el número máximo de consumidores por cola
    private processMap = new Map<string, { callback: (job: IJobData, done: (err?: Error) => void) => void, nConsumers: number }>();

    private monitorInterval: NodeJS.Timeout | null = null;
    private isClosing = false;

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
        this.startMonitor();
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

    private async getNextGroupWithJobs(queueName: string): Promise<string | null> {
        const client = await this.pool.acquire();
        try {
            const pattern = `qube:${queueName}:group:*`;
            const keys = await client.keys(pattern);
            if (keys.length === 0) return null;

            // Seleccionar un grupo aleatoriamente para distribuir la carga equitativamente
            return keys[Math.floor(Math.random() * keys.length)].split(':').pop()!;
        } finally {
            this.pool.release(client);
        }
    }

    private delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

    private async dequeueJob(queueName: string, groupName: string): Promise<IJobData | null> {
        const groupQueueKey = `qube:${queueName}:group:${groupName}`;
        const result = await this.executeScript('dequeue', [groupQueueKey]);
        if (!result) return null;
        const [jobId, jobDataRaw, groupNameFromJob] = result;
        const jobDataParsed = jobDataRaw ? JSON.parse(jobDataRaw) : null;
        return {
            id: jobId,
            data: jobDataParsed,
            groupName: groupNameFromJob ? groupNameFromJob.toString() : '',
            progress: async (value: number) => await this.updateProgress(jobId, value),
        };
    }

    // Eliminamos el loop genérico en process()
    public async process(queueName: string, nConsumers: number, callback: (job: IJobData, done: (err?: Error) => void) => void): Promise<void> {
        this.logger.info(`Registrando procesadores para la cola '${queueName}' con ${nConsumers} consumidores por grupo`);
        this.processMap.set(queueName, { callback, nConsumers });
        // Se elimina el loop genérico. Los consumidores se iniciarán a través del subscriber al publicarse un trabajo.
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
                const groups = keys.map(key => key.split(':').pop()!);
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

        this.monitorInterval = setInterval(async () => {
            this.logger.info('Ejecutando tarea periódica cada 10 segundos');

            for (const [queueName, { nConsumers }] of this.processMap.entries()) {
                this.logger.info(`Revisando la cola: ${queueName} con ${nConsumers} consumidores`);

                const groupNames = await this.getGroupNames(queueName);

                for (const groupName of groupNames) {
                    const client = await this.pool.acquire();
                    try {
                        // Escanear los consumidores activos
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
                                    await this.removeConsumer(queueName, groupName, workerId);
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

    private async acquireLock(queueName: string, groupName: string, workerId: string): Promise<boolean> {
        const lockKey = `qube:lock:${queueName}:${groupName}`;
        const ttl = 30;
        const client = await this.pool.acquire();
        try {
            const result = await client.set(lockKey, workerId, 'EX', ttl, 'NX');
            return result === 'OK';
        } finally {
            this.pool.release(client);
        }
    }

    private async releaseLock(queueName: string, groupName: string, workerId: string): Promise<void> {
        const client = await this.pool.acquire();
        const lockKey = `qube:lock:${queueName}:${groupName}`;
        try {
            const currentWorkerId = await client.get(lockKey);
            if (currentWorkerId === workerId) {
                await client.del(lockKey);
            }
        } finally {
            this.pool.release(client);
        }
    }

    // Se intenta registrar un consumidor para un grupo
    private async canStartConsumer(queueName: string, groupName: string, maxConsumers: number): Promise<string | null> {
        // Revisamos el arreglo actual de consumidores para ese grupo
        let consumers = this.groupConsumers.get(groupName) || [];
        if (consumers.length >= maxConsumers) {
            this.logger.info(`❌ Máximo de consumidores alcanzado para ${queueName}:${groupName}`);
            return null;
        }
        const workerId = Math.random().toString(36).substr(2, 9);
        const ttl = 30;
        const keyPrefix = `qube:consumer:${queueName}:${groupName}`;
        const client = await this.pool.acquire();
        try {
            const sha = this.scriptShas.get('limit_consumers');
            if (!sha) throw new Error('El script limit_consumers no está cargado.');
            const result = await client.evalsha(sha, 1, keyPrefix, maxConsumers, workerId, ttl);
            if (result === 1 && await this.acquireLock(queueName, groupName, workerId)) {
                this.logger.info(`✅ Consumidor registrado para grupo '${groupName}' con ID: ${workerId}`);
                consumers.push(workerId);
                this.groupConsumers.set(groupName, consumers);
                return workerId;
            } else {
                this.logger.info(`❌ No se pudo iniciar consumidor para ${queueName}:${groupName}`);
                return null;
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
            await client.set(consumerKey, 'active', 'EX', ttl);
            this.logger.info(`Consumidor registrado: ${consumerKey}`);
        } finally {
            this.pool.release(client);
        }
    }

    private async groupWorker(queueName: string, groupName: string, workerId: string): Promise<void> {
        const client = await this.pool.acquire();
        const ttl = 30; // TTL en segundos
        const refreshInterval = 10; // Renovar cada 10 segundos

        // Registrar el consumidor para el grupo (ya se hizo en canStartConsumer)
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
                    // Se valida que el trabajo corresponda al grupo asignado
                    if (groupNameFromJob.toString() !== groupName) {
                        this.logger.warn(`⚠️ Trabajo recibido para grupo ${groupNameFromJob} en consumidor de grupo ${groupName}. Ignorando.`);
                        continue;
                    }
                    const jobDataParsed = jobDataRaw ? JSON.parse(jobDataRaw) : null;
                    const job: IJobData = {
                        id: jobId,
                        data: jobDataParsed,
                        groupName: groupNameFromJob.toString(),
                        progress: async (value: number) => await this.updateProgress(jobId, value)
                    };
                    const processData = this.processMap.get(queueName);
                    if (!processData) {
                        this.logger.warn(`No se encontró configuración para la cola ${queueName}`);
                        break; // O return, según la lógica que necesites
                    }
                    await this.processJob(job, processData.callback);
                } else {
                    break; // No quedan más trabajos en ese grupo
                }
            }
        } finally {
            clearInterval(renewTTLInterval);
            await this.removeConsumer(queueName, groupName, workerId);
            // Eliminar este consumidor del arreglo para ese grupo
            let consumers = this.groupConsumers.get(groupName) || [];
            this.groupConsumers.set(groupName, consumers.filter(id => id !== workerId));
            await this.pool.release(client);
        }
    }

    private setupSubscriber(): void {
        this.subscriberClient.on('message', async (channel: string, message: string) => {
            if (channel === 'QUEUE:NEWJOB') {
                const { queueName, groupName } = JSON.parse(message);
                const processData = this.processMap.get(queueName);
                if (!processData) {
                    this.logger.warn(`⚠️ No hay callback registrado para la cola '${queueName}', ignorando.`);
                    return;
                }
                const { nConsumers } = processData;
                // Se usa el groupName recibido para iniciar (o reutilizar) un consumidor exclusivo para ese grupo.
                const workerId = await this.canStartConsumer(queueName, groupName, nConsumers);
                if (!workerId) {
                    this.logger.info(`❌ No se pudo iniciar consumidor para ${queueName}:${groupName}`);
                    return;
                }
                this.logger.info(`✅ Iniciando consumidor para ${queueName}:${groupName} con ID: ${workerId}`);
                await this.startGroupConsumer(queueName, groupName, workerId);
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

    // Nuevo: Método para detener el monitor manualmente
    public stopMonitor(): void {
        if (this.monitorInterval) {
            clearInterval(this.monitorInterval);
            this.monitorInterval = null;
            this.logger.info('Monitor detenido.');
        }
    }

    public async close(): Promise<void> {
        this.isClosing = true;
        this.logger.info('Cerrando conexiones...');
        await this.client.quit();
        await this.publisherClient.quit();
        await this.subscriberClient.quit();
        await this.pool.drain();
        await this.pool.clear();
    }

}