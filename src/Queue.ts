import Redis, { RedisOptions } from 'ioredis';
import { createPool, Pool } from 'generic-pool';
import { v4 as uuidv4 } from 'uuid';

import { promises as fs } from 'fs';
import { join } from 'path';

import { Logger } from './utils/logger';
import { createRedisPool } from './utils/redisPool';
import { LuaScriptLoader } from './utils/LuaScriptLoader';
import { IJobData, ICallback, IProcessEntry, IQueueInitOptions, ILuaScriptResult, ILastTaskTime } from './interfaces/queue.interface';

interface IRedisManager {
    credentials: RedisOptions,
    scripts: string[],
    logLevel: string
}

// Esta clase gestionara todos los temas relacionados con redis, incluyendo la carga y procesamiento de los script lua
class RedisManager {

    private logger: Logger;

    private pool: Pool<Redis>;

    private scripts: string[];

    private scriptsDir: string = "scripts"

    private scriptShas: Map<string, string> = new Map();

    constructor(options: IRedisManager) {

        this.pool = createPool(
            {
                create: async () => new Redis(options.credentials),
                destroy: async (client) => {
                    await client.quit();
                }
            },
            {
                max: 1000,
                min: 5
            }
        );

        this.scripts = options.scripts;

        this.logger = new Logger(options.logLevel);

    }

    public async init(): Promise<void> {
        this.logger.info('Iniciando la cola y cargando scripts LUA...');
        try {
            await this.loadAndRegisterLuaScripts(this.scripts);
            this.logger.info('✅ Todos los scripts LUA se han cargado correctamente.');
        } catch (error) {
            this.logger.error(`❌ Error al cargar los scripts LUA: ${(error as Error).message}`);
            throw error;
        }
    }

    private async loadAndRegisterLuaScripts(scriptNames: string[]): Promise<void> {
        this.logger.info('Cargamos los scripts LUA.');
        for (const name of scriptNames) {
            await this.loadLuaScript(name);
            const scriptContent = this.getLuaScript(name);
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

    async loadLuaScript(scriptName: string): Promise<void> {
        const filePath = join(__dirname, '..', this.scriptsDir, `${scriptName}.lua`);
        try {
            const scriptContent = await fs.readFile(filePath, 'utf8');
            this.scriptShas.set(scriptName, scriptContent);
        } catch (error) {
            console.error(`Error loading script ${scriptName}:`, error);
        }
    }

    private getLuaScript(scriptName: string): string | undefined {
        return this.scriptShas.get(scriptName);
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

class Cleaner {

    constructor() {

    }

}

// Clase encargada de la gestion de los trabajos
class Job {

    constructor() {

    }

}

// Esta clase es la encargada de gestionar la logica y las opciones de un consumidor en especial, tener en cuenta que cuando se llama a run es porque ya un script LUA creo un registro en redis, entonces si este consumidor falla entonces tenemos que eliminar ese registro en redis para evitar tener registros inecesarios.
class Consumer {

    // Este uid identfica esta instancia de consumidor de una lista de instancias de consumidores
    public uid: string = uuidv4();

    public status: 'SLEEPING' | 'RUNNING' = 'SLEEPING'; // SLEEPING = No esta procesando trabajos, RUNNING = Esta procesando trabajos.

    private qsession: string;

    private queueName: string;

    private callback: ICallback;

    constructor(qsession: string, queueName: string, callback: ICallback) {

        this.qsession = qsession;
        this.queueName = queueName;
        this.status = 'SLEEPING';
        this.callback = callback;
    }

    public run = (): void => {
        if (this.status === 'RUNNING') {
            throw new Error(`❎ El consumidor de la cola ${this.queueName} con sesión ${this.qsession} ya está en ejecución. No es posible iniciar nuevamente este consumidor a menos que esté en estado de espera.`);
        }
        this.status = 'RUNNING';
        // Ejecutamos callback
    }

}

// QUEUE:GROUPNAME:SESSIONID

export class Queue {

    private uid: string;

    private logger: Logger;

    private publisherClient: Redis;

    private subscriberClient: Redis;

    private redisManager: RedisManager;

    private consumerLimits?: Record<string, number>;

    private processMap = new Map<string, IProcessEntry>();

    // Almacenar los consumidores creados
    private consumers = new Map<string, Consumer>();

    constructor(options: IQueueInitOptions) {

        this.logger = new Logger(options.logLevel);

        this.publisherClient = new Redis(options.credentials);

        this.subscriberClient = new Redis(options.credentials);

        this.consumerLimits = options.consumerLimits;

        this.redisManager = new RedisManager({
            credentials: options.credentials,
            scripts: ["dequeue", "enqueue", "get_status", "update_status"],
            logLevel: "debug"
        })

        // Agregamos esta variable para asignarle un id a esta instancia de esta clase
        this.uid = uuidv4()

        // Nos suscribimos a los eventos de nuevos mensajes y a los eventos de esta sesion
        this.setupSubscriber();
    }

    private setupSubscriber(): void {
        // Nos sucribimos a los canales de mensajeria de la libreria

    }

    public add(queueName: string, groupName: string, data: any) {
        // Publicamos el trabajo usando el script lua.

        // Notificamos a todos los clientes del nuevo trabjo.

    }

    public async process(queueName: string, callback: ICallback): Promise<void> {
        this.logger.info(`Registrando procesador para la cola '${queueName}'.`);
        // Agregamos a la pila de consumers de esta instancia este metodo para esata cola.
    }

    public async close(): Promise<void> {
        this.logger.info('Cerrando conexiones...');
        await this.publisherClient.quit();
        await this.subscriberClient.quit();
    }

}