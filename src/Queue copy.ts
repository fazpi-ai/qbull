import Redis, { RedisOptions } from 'ioredis';
import { createPool, Pool } from 'generic-pool';
import { Logger } from './utils/logger';
import { createRedisPool } from './utils/redisPool';
import { LuaScriptLoader } from './utils/LuaScriptLoader';
import { IJobData, ICallback, IProcessEntry, IQueueInitOptions, ILuaScriptResult } from './interfaces/queue.interface';

export class Queue {
    private client: Redis;
    private pool: Pool<Redis>;
    private logger: Logger;
    private scriptLoader: LuaScriptLoader;
    private scriptShas: Map<string, string> = new Map();

    private publisherClient: Redis;
    private subscriberClient: Redis;

    private consumerLimits?: Record<string, number>;

    private processMap = new Map<string, IProcessEntry>();

    constructor(options: IQueueInitOptions) {
        this.logger = new Logger(options.logLevel);
        this.client = new Redis(options.credentials);
        this.pool = createRedisPool(options.credentials);
        this.scriptLoader = new LuaScriptLoader("lua-scripts");
        this.publisherClient = new Redis(options.credentials);
        this.subscriberClient = new Redis(options.credentials);
        this.consumerLimits = options.consumerLimits;

        this.setupSubscriber();
    }

    public async init(): Promise<void> {
        this.logger.info('Iniciando la cola y cargando scripts LUA...');
        try {
            await this.loadAndRegisterScripts([
                'enqueue',
                'dequeue',
                'update_status',
                'get_status',
                'limit_consumers'
            ]);
            this.logger.info('✅ Todos los scripts LUA se han cargado correctamente.');
        } catch (error) {
            this.logger.error(`❌ Error al cargar los scripts LUA: ${(error as Error).message}`);
            throw error;
        }
    }

    private async loadAndRegisterScripts(scriptNames: string[]): Promise<void> {
        this.logger.info('Cargamos los scripts LUA.');
        for (const name of scriptNames) {
            await this.scriptLoader.loadScript(name);
            const scriptContent = this.scriptLoader.getScript(name);
            this.logger.info(`Cargamos el script lua ${name}.`);
            if (scriptContent) {
                const client = await this.pool.acquire();
                try {
                    const sha = await client.script('LOAD', scriptContent) as string;
                    this.logger.info(`SHA generado para ${name}: ${sha}`);
                    this.scriptShas.set(name, sha);
                } finally {
                    this.pool.release(client);
                }
            } else {
                this.logger.error(`No se pudo cargar el script: ${name}`);
            }
        }
    }

    private async executeScript(
        scriptName: string,
        keys: string[] = [],
        args: (string | number)[] = []
    ): Promise<ILuaScriptResult | number | null> {  // 🔹 Ahora puede devolver null
        let sha = this.scriptShas.get(scriptName);
        if (!sha) {
            console.warn(`El script ${scriptName} no está registrado. Intentando recargar...`);
            await this.loadAndRegisterScripts([scriptName]);  // Recarga solo este script
            sha = this.scriptShas.get(scriptName);

            if (!sha) {
                throw new Error(`❌ No se pudo registrar el script ${scriptName} después del intento de recarga.`);
            }
        }

        const result = await this.client.evalsha(sha, keys.length, ...keys, ...args);

        // 🔹 Si el resultado es null, lo devolvemos tal cual sin error
        if (result === null) {
            this.logger.info(`ℹ️ El script ${scriptName} devolvió null (no hay trabajos disponibles).`);
            return null;
        }

        // 🔹 Si el resultado es un número (para enqueue.lua)
        if (typeof result === "number" || typeof result === "string") {
            this.logger.info(`✅ El script ${scriptName} devolvió un jobId: ${result}`);
            return Number(result);
        }

        // 🔹 Si el resultado es un array (para dequeue.lua)
        if (Array.isArray(result) && result.length >= 3) {
            return {
                jobId: Number(result[0]),
                jobData: typeof result[1] === 'string' ? JSON.parse(result[1]) : result[1],
                groupName: String(result[2])
            };
        }

        throw new Error(`El script ${scriptName} devolvió un resultado inesperado: ${JSON.stringify(result)}`);
    }

    public async getActiveConsumersCount(queueName: string, groupName: string): Promise<number> {
        const client = await this.pool.acquire();
        try {
            const consumerSetKey = `qube:consumer:${queueName}:${groupName}`;
            return await client.scard(consumerSetKey); // Contar consumidores activos en el set
        } finally {
            this.pool.release(client);
        }
    }

    // Este metodo se encarga de registrar la key del consumidor
    private async registerConsumer(queueName: string, groupName: string): Promise<string> {
        const client = await this.pool.acquire();
        try {
            const workerId = Math.random().toString(36).substring(2, 11);
            const ttl = 30; // TTL en segundos
            const consumerSetKey = `qube:consumer:${queueName}:${groupName}`;
            const consumerTTLKey = `qube:consumerTTL:${queueName}:${groupName}`;

            await client.sadd(consumerSetKey, workerId);
            await client.hset(consumerTTLKey, workerId, ttl);
            await client.expire(consumerTTLKey, ttl); // Expira el hash global

            this.logger.info(`Consumidor registrado en set: ${workerId} (TTL: ${ttl}s)`);
            return workerId;
        } catch (error) {
            this.logger.error(`Error registrando consumidor en ${queueName}:${groupName}: ${(error as Error).message}`);
            throw error;
        } finally {
            this.pool.release(client);
        }
    }

    private async updateProgress(jobId: number, value: number): Promise<void> {
        const client = await this.pool.acquire();
        try {
            await client.hset(`qube:queue:job:${jobId}`, 'progress', value);
        } finally {
            this.pool.release(client);
        }
    }

    private async updateJobStatus(jobId: number, newStatus: string) {
        await this.executeScript('update_status', [], [jobId, newStatus]);
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

    // Este metodo se encarga de levantar un nuevo consumidor para el grupo
    private async startGroupConsumer(queueName: string, groupName: string): Promise<void> {
        const client = await this.pool.acquire();
        const refreshInterval = 10 * 1000;
        try {
            const workerId = await this.registerConsumer(queueName, groupName);
            const consumerSetKey = `qube:consumer:${queueName}:${groupName}`;
            const consumerTTLKey = `qube:consumerTTL:${queueName}:${groupName}`;

            console.log(`Iniciamos un heartbeat para ${queueName}:${groupName}`);
            const heartbeat = setInterval(async () => {
                try {
                    // Verificar si el consumidor aún está en el set
                    const isActive = await client.sismember(consumerSetKey, workerId);
                    if (!isActive) {
                        clearInterval(heartbeat);
                        await client.hdel(consumerTTLKey, workerId);
                        this.logger.info(`🛑 Consumidor eliminado: ${workerId}`);
                        return;
                    }

                    // Renovar TTL
                    await client.hset(consumerTTLKey, workerId, 30);
                    this.logger.info(`🔄 TTL renovado para ${workerId}`);

                } catch (error) {
                    this.logger.error(`❌ Error renovando TTL para ${workerId}: ${(error as Error).message}`);
                }
            }, refreshInterval);

            // Procesamiento de trabajos
            while (true) {
                const groupQueueKey = `qube:${queueName}:group:${groupName}`;
                const result = await this.executeScript('dequeue', [groupQueueKey], []) as ILuaScriptResult;

                if (result) {
                    const job: IJobData = {
                        id: result.jobId,
                        data: result.jobData,
                        groupName: result.groupName,
                        progress: async (value: number) => await this.updateProgress(result.jobId, value)
                    };

                    const processData = this.processMap.get(queueName);
                    if (!processData) {
                        this.logger.warn(`No se encontró configuración para la cola ${queueName}`);
                        break;
                    }
                    await this.processJob(job, processData.callback);
                } else {
                    this.logger.info(`ℹ️ No hay más trabajos en la cola ${queueName}:${groupName}. Finalizando consumidor.`);
                    break;
                }
            }

            clearInterval(heartbeat);
            await client.srem(consumerSetKey, workerId);
            await client.hdel(consumerTTLKey, workerId);
            this.logger.info(`🛑 Consumidor finalizado y eliminado: ${workerId}`);

        } finally {
            this.pool.release(client);
        }
    }

    private setupSubscriber(): void {
        this.subscriberClient.on('message', async (channel: string, message: string) => {
            if (channel === 'QUEUE:NEWJOB') {
                const { queueName, groupName } = JSON.parse(message);
                this.logger.info(`Recibimos un nuevo mensaje de QUEUE:NEWJOB ${queueName}:${groupName}.`);
                const processData = this.processMap.get(queueName);
                if (!processData) {
                    this.logger.warn(`⚠️ No hay callback registrado para la cola '${queueName}', ignorando.`);
                    return;
                }

                const nConsumers = this.consumerLimits?.[queueName] || 1;
                this.logger.info(`Numero de consumidores maximos para ${queueName} es ${nConsumers}.`);

                this.logger.info(`Preguntamos a REDIS cuantos consumidores hay para ${queueName}.`);
                const consumersCount = await this.getActiveConsumersCount(queueName, groupName);
                this.logger.info(`Para ${queueName} tenemos ${consumersCount} consumidores.`);

                if (consumersCount === 0) { //  || consumersCount < nConsumers
                    this.startGroupConsumer(queueName, groupName);
                }
            }
        });
        this.logger.info('Nos suscribimos a redis para recibir notificaciones.');
        this.subscriberClient.subscribe('QUEUE:NEWJOB', (err, count) => {
            if (err) {
                this.logger.error(`Error suscribiéndose a 'QUEUE:NEWJOB': ${err.message}`);
            } else {
                this.logger.info(`Suscrito a ${count} canal(es), esperando mensajes...`);
            }
        });
    }

    public async add(queueName: string, groupName: string, data: any): Promise<number> {
        const keys = [`qube:${queueName}:groups`, `qube:${queueName}:group:${groupName}`];
        const args = [JSON.stringify(data), groupName];

        try {
            const result = await this.executeScript('enqueue', keys, args);

            if (typeof result !== "number") {
                throw new Error(`Error: enqueue.lua devolvió un valor inesperado: ${JSON.stringify(result)}`);
            }

            this.logger.info(`Trabajo encolado con jobId: ${result}`);

            await this.publisherClient.publish(`QUEUE:NEWJOB`, JSON.stringify({ queueName, groupName }));

            return result;
        } catch (error) {
            this.logger.error(`Error en add(): ${(error as Error).message}`);
            throw error;
        }
    }

    // Eliminamos el loop genérico en process()
    public async process(queueName: string, callback: ICallback): Promise<void> {
        this.logger.info(`Registrando procesador para la cola '${queueName}'.`);
        this.processMap.set(queueName, { callback });

        const queueGroupsKey = `qube:${queueName}:groups`;
        const client = await this.pool.acquire();
        try {
            const groupKeys = await client.smembers(queueGroupsKey);
            for (const groupKey of groupKeys) {
                const groupName = groupKey.split(':').pop();
                if (!groupName) continue;
                const pendingJobs = await client.llen(groupKey);
                if (pendingJobs > 0) {
                    this.logger.info(`📌 Hay ${pendingJobs} trabajos pendientes en '${queueName}:${groupName}', iniciando consumidores.`);
                    this.startGroupConsumer(queueName, groupName);
                }
            }
        } finally {
            this.pool.release(client);
        }
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