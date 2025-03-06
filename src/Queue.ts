import Redis, { RedisOptions } from 'ioredis';
import pino from 'pino';
import { createPool, Pool } from 'generic-pool';
import { v4 as uuidv4 } from 'uuid';

import { promises as fs } from 'fs';
import { join } from 'path';

import { IJobData, ICallback, IProcessEntry, IQueueInitOptions, ILuaScriptResult, ILastTaskTime } from './interfaces/queue.interface';

interface IRedisManager {
    credentials: RedisOptions,
    scripts: string[],
    logLevel: string
}

class Logger {
    private logger;

    constructor(level: string) {
        this.logger = pino({ level });
    }

    info(message: string) {
        this.logger.info(`${message}`);
    }

    error(message: string) {
        this.logger.error(`${message}`);
    }

    warn(message: string) {
        this.logger.warn(`${message}`);
    }
}

// Esta clase gestionara todos los temas relacionados con redis, incluyendo la carga y procesamiento de los script lua
class RedisManager {
    private logger: Logger;
    private pool: Pool<Redis>;
    private scripts: string[] = ["dequeue", "enqueue", "get_status", "update_status"];
    private scriptsDir: string = "src/scripts";
    private scriptContents: Map<string, string> = new Map();
    private scriptShas: Map<string, string> = new Map();

    constructor(options: IRedisManager) {
        this.pool = createPool(
            {
                create: async () => new Redis(options.credentials),
                destroy: async (client: Redis) => {
                    await client.quit();
                    return Promise.resolve();
                },
            },
            {
                max: 1000,
                min: 5,
            }
        );

        this.scripts = options.scripts;

        this.logger = new Logger(options.logLevel);
    }

    // obtener el script sha por el nombre del script
    public getScriptSha = (scriptName: string): string | undefined => {
        return this.scriptShas.get(scriptName);
    }

    public async init(): Promise<void> {
        this.logger.info('Iniciando la cola y cargando scripts LUA...');
        try {
            await this.loadAndRegisterLuaScripts();
            this.logger.info('✅ Todos los scripts LUA se han cargado correctamente.');
        } catch (error) {
            this.logger.error(`❌ Error al cargar los scripts LUA: ${(error as Error).message}`);
            throw error;
        }
    }

    private async loadAndRegisterLuaScripts(): Promise<void> {
        this.logger.info('Cargando scripts LUA...');
        for (const name of this.scripts) {
            const scriptContent = await this.loadLuaScript(name);
            if (scriptContent) {
                const client = await this.pool.acquire();
                try {
                    const sha = await client.script('LOAD', scriptContent) as string;
                    this.scriptShas.set(name, sha);
                    this.logger.info(`SHA generado para ${name}: ${sha}`);
                } finally {
                    this.pool.release(client);
                }
            } else {
                this.logger.error(`No se pudo cargar el script: ${name}`);
            }
        }
    }

    private async loadLuaScript(scriptName: string): Promise<string | null> {
        const filePath = join(__dirname, '..', this.scriptsDir, `${scriptName}.lua`);
        try {
            const scriptContent = await fs.readFile(filePath, 'utf8');
            this.scriptContents.set(scriptName, scriptContent);
            return scriptContent;
        } catch (error) {
            this.logger.error(`Error cargando script ${scriptName}: ${(error as Error).message}`);
            return null;
        }
    }

    public async getClient(): Promise<Redis> {
        return this.pool.acquire();
    }

    public async releaseClient(client: Redis): Promise<void> {
        this.pool.release(client);
    }

    public async quit(): Promise<void> {
        await this.pool.drain();
        await this.pool.clear();
    }
}

// Clase que representa un consumidor de la cola
class Consumer {
    public uid: string = uuidv4();
    public status: 'SLEEPING' | 'RUNNING' = 'SLEEPING';
    private queueName: string;
    private callback: ICallback;

    constructor(private qsession: string, queueName: string, callback: ICallback) {
        this.queueName = queueName;
        this.callback = callback;
    }

    public async run(jobData: IJobData): Promise<void> {
        if (this.status === 'RUNNING') {
            throw new Error(`❎ El consumidor de la cola ${this.queueName} ya está en ejecución.`);
        }
        this.status = 'RUNNING';

        try {
            await new Promise<void>((resolve, reject) => {
                this.callback(jobData, (err?: Error) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        } catch (error: unknown) {
            if (error instanceof Error) {
                console.error(`❌ Error en el consumidor de ${this.queueName}: ${error.message}`);
            } else {
                console.error(`❌ Error desconocido en el consumidor de ${this.queueName}: ${String(error)}`);
            }
        } finally {
            this.status = 'SLEEPING';
        }
    }
}

// QUEUE:GROUPNAME:SESSIONID

// Clase principal de la cola de procesamiento
export class Queue {
    private uid: string;
    private logger: Logger;
    private subscriberClient?: Redis;
    private publisherClient?: Redis;
    private type: 'publisher' | 'subscriber';
    private redisManager: RedisManager;
    private consumerLimits?: Record<string, number>;
    private consumers = new Map<string, Consumer>();
    private activeProcessingInterval: NodeJS.Timeout | null = null;

    constructor(options: IQueueInitOptions) {
        this.uid = uuidv4();
        this.logger = new Logger(options.logLevel);
        this.redisManager = new RedisManager({
            credentials: options.credentials,
            scripts: ["dequeue", "enqueue", "get_status", "update_status"],
            logLevel: "debug"
        });

        this.logger.info(`🔑 Iniciando cola con UID: ${this.uid}.`);

        this.type = options.type;
        this.consumerLimits = options.consumerLimits;

        if (options.type === 'subscriber') {
            this.subscriberClient = new Redis(options.credentials);
            this.setupSubscriber();
        }

        if (options.type === 'publisher') {
            this.publisherClient = new Redis(options.credentials);
        }
    }

    public init = async () => {
        await this.redisManager.init();
        
        // Si es un suscriptor, iniciar el procesamiento activo de tareas
        if (this.type === 'subscriber') {
            this.startActiveProcessing();
        }
    }

    private startActiveProcessing(): void {
        // Evitar iniciar múltiples intervalos
        if (this.activeProcessingInterval) {
            clearInterval(this.activeProcessingInterval);
        }
        
        // Procesar activamente las tareas cada 5 segundos
        this.activeProcessingInterval = setInterval(async () => {
            // Procesar cada cola para la que tenemos un consumidor registrado
            for (const [queueName, consumer] of this.consumers.entries()) {
                // Solo procesar si el consumidor no está ocupado
                if (consumer.status === 'SLEEPING') {
                    await this.processQueueTasks(queueName, consumer);
                }
            }
        }, 5000); // Intervalo de 5 segundos
    }

    private async processQueueTasks(queueName: string, consumer: Consumer): Promise<void> {
        const client = await this.redisManager.getClient();
        try {
            // Obtener el SHA del script dequeue
            const scriptSha = this.redisManager.getScriptSha('dequeue');
            if (!scriptSha) {
                this.logger.error(`❌ No se pudo obtener el SHA del script "dequeue" para la cola ${queueName}.`);
                return;
            }

            // Obtener todas las claves de grupo para esta cola
            const queueGroupsKey = `qube:${queueName}:groups`;
            const groupKeys = await client.smembers(queueGroupsKey);

            // Procesar cada grupo
            for (const groupKey of groupKeys) {
                // Ejecutar el script dequeue para obtener el trabajo
                const result = await client.evalsha(scriptSha, 1, groupKey);
                
                if (result && Array.isArray(result)) {
                    // El resultado es un array con [job_id, job_data, group_name]
                    const [jobId, jobDataStr, groupName] = result;
                    
                    // Parsear los datos del trabajo
                    const jobData = {
                        id: jobId,
                        data: JSON.parse(jobDataStr),
                        groupName: groupName,
                        progress: async (value: number) => {
                            // Implementación de la función progress
                            this.logger.info(`Progreso del trabajo ${jobId}: ${value}%`);
                        }
                    };
                    
                    // Ejecutar el consumidor con los datos del trabajo
                    this.logger.info(`🔄 Procesando trabajo ${jobId} de la cola ${queueName}, grupo ${groupName}`);
                    await consumer.run(jobData);
                    
                    // Solo procesar un trabajo a la vez por consumidor
                    break;
                }
            }
        } catch (error) {
            if (error instanceof Error) {
                this.logger.error(`❌ Error al procesar tareas de la cola ${queueName}: ${error.message}`);
            } else {
                this.logger.error(`❌ Error desconocido al procesar tareas de la cola ${queueName}: ${String(error)}`);
            }
        } finally {
            this.redisManager.releaseClient(client);
        }
    }

    public getUid = () => {
        return this.uid;
    }

    private setupSubscriber(): void {
        if (this.subscriberClient) {
            this.subscriberClient.subscribe('QUEUE:NEWJOB', (err) => {
                if (err) {
                    this.logger.error(`❌ Error al suscribirse al canal de trabajos: ${err.message}`);
                } else {
                    this.logger.info(`📡 Suscrito al canal de trabajos desde ${this.uid}.`);
                }
            });

            this.subscriberClient.on('message', async (channel, message) => {
                this.logger.info(`📨 Mensaje recibido en ${channel}: ${message} desde ${this.uid}.`);
                const jobInfo = JSON.parse(message);

                // Verificar si tenemos un consumidor para esta cola
                const consumer = this.consumers.get(jobInfo.queueName);
                if (consumer) {
                    try {
                        // Obtener un cliente Redis para dequeue
                        const client = await this.redisManager.getClient();
                        try {
                            // Obtener el SHA del script dequeue
                            const scriptSha = this.redisManager.getScriptSha('dequeue');
                            if (!scriptSha) {
                                this.logger.error('❌ No se pudo obtener el SHA del script "dequeue".');
                                return;
                            }

                            // Construir la clave del grupo
                            const groupKey = `qube:${jobInfo.queueName}:group:${jobInfo.groupName}`;
                            
                            // Ejecutar el script dequeue para obtener el trabajo
                            const result = await client.evalsha(scriptSha, 1, groupKey);
                            
                            if (result && Array.isArray(result)) {
                                // El resultado es un array con [job_id, job_data, group_name]
                                const [jobId, jobDataStr, groupName] = result;
                                
                                // Parsear los datos del trabajo
                                const jobData = {
                                    id: jobId,
                                    data: JSON.parse(jobDataStr),
                                    groupName: groupName,
                                    queueName: jobInfo.queueName,
                                    progress: async (value: number) => {
                                        // Implementación de la función progress
                                        this.logger.info(`Progreso del trabajo ${jobId}: ${value}%`);
                                    }
                                };
                                
                                // Ejecutar el consumidor con los datos del trabajo
                                this.logger.info(`🔄 Procesando trabajo ${jobId} de la cola ${jobInfo.queueName}`);
                                await consumer.run(jobData);
                            } else {
                                this.logger.info(`⚠️ No hay trabajos pendientes en la cola ${jobInfo.queueName}, grupo ${jobInfo.groupName}`);
                            }
                        } finally {
                            // Liberar el cliente Redis
                            this.redisManager.releaseClient(client);
                        }
                    } catch (error) {
                        if (error instanceof Error) {
                            this.logger.error(`❌ Error al procesar trabajo: ${error.message}`);
                        } else {
                            this.logger.error(`❌ Error desconocido al procesar trabajo: ${String(error)}`);
                        }
                    }
                } else {
                    this.logger.warn(`⚠️ No hay consumidor registrado para la cola ${jobInfo.queueName}`);
                }
            });
        } else {
            this.logger.error('❌ No se pudo suscribir al canal de trabajos.');
        }
    }

    public async add(queueName: string, groupName: string, data: any): Promise<any> {
        this.logger.info(`Agregando trabajo '${queueName}' al grupo '${groupName}' con datos: ${JSON.stringify(data)}`);
        const client = await this.redisManager.getClient();
        try {
            const scriptSha = this.redisManager.getScriptSha('enqueue') as string;
            if (!scriptSha) {
                this.logger.error('❌ No se pudo obtener el SHA del script "enqueue".');
                return null;
            }
            // Convertimos explícitamente los argumentos a string
            const queueKey = String(`qube:${queueName}:groups`);
            const groupKey = String(`qube:${queueName}:group:${groupName}`);
            const jobId = await client.evalsha(scriptSha, 2, queueKey, groupKey, String(JSON.stringify(data)), groupName);
            if (this.publisherClient) {
                await this.publisherClient.publish('QUEUE:NEWJOB', JSON.stringify({ queueName, groupName }));
            }
            return jobId;
        } catch (error: unknown) {
            if (error instanceof Error) {
                this.logger.error(`❌ Error al agregar trabajo: ${error.message}`);
            } else {
                this.logger.error(`❌ Error al agregar trabajo: ${String(error)}`);
            }
            return null;
        } finally {
            this.redisManager.releaseClient(client);
        }
    }

    public async process(queueName: string, callback: ICallback): Promise<void> {
        this.logger.info(`Registrando procesador para la cola '${queueName}' desde ${this.uid}.`);
        if (this.consumers.has(queueName)) {
            throw new Error(`Ya existe un procesador para la cola '${queueName}'.`);
        }

        const consumer = new Consumer(uuidv4(), queueName, callback);
        this.consumers.set(queueName, consumer);
        
        // Procesar inmediatamente las tareas pendientes para esta cola
        if (this.type === 'subscriber') {
            await this.processQueueTasks(queueName, consumer);
        }
    }

    public async close(): Promise<void> {
        this.logger.info('Cerrando conexiones...');
        
        // Limpiar el intervalo de procesamiento activo
        if (this.activeProcessingInterval) {
            clearInterval(this.activeProcessingInterval);
            this.activeProcessingInterval = null;
        }
        
        await this.redisManager.quit();
    }
}