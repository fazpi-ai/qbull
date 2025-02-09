import Redis from 'ioredis';
import { createPool, Pool } from 'generic-pool';
import { RedisCredentials } from './interfaces/redis';
import pino from 'pino';

const logger = pino({ level: 'debug' });

export default class Queue<DataType extends Record<string, string>> {
    private pool!: Pool<Redis>;
    private processMap: Map<string, (message: DataType) => Promise<void>> = new Map();
    private nConsumersMap: Map<string, number> = new Map();
    private activeConsumers: Map<string, Map<string, { busy: boolean; lastActive: number }>> = new Map();

    constructor(private credentials: RedisCredentials) {
        const factory = {
            create: async () => {
                const client = new Redis({
                    host: this.credentials.host,
                    port: this.credentials.port,
                    password: this.credentials.password,
                    db: this.credentials.db,
                });
                client.on('connect', () => logger.debug('Conexión a Redis establecida'));
                return client;
            },
            destroy: async (client: Redis) => {
                client.disconnect();
                logger.debug('Conexión a Redis cerrada');
            },
        };
        this.pool = createPool(factory, { max: 10, min: 2 });
        logger.debug('Pool de conexiones creado');
    }

    async getClient(): Promise<Redis> {
        logger.debug('Adquiriendo cliente de Redis');
        return this.pool.acquire();
    }

    releaseClient(client: Redis): void {
        this.pool.release(client);
        logger.debug('Cliente de Redis liberado');
    }

    async initStream(streamName: string, group: string): Promise<void> {
        const client = await this.getClient();
        try {
            await client.xgroup('CREATE', streamName, group, '$', 'MKSTREAM');
            logger.debug(`Grupo '${group}' creado en el stream '${streamName}'`);
        } catch (error: any) {
            if (!error.message.includes('BUSYGROUP')) {
                logger.error('Error al crear el grupo:', error);
                throw error;
            } else {
                logger.debug(`El grupo '${group}' ya existe en el stream '${streamName}'`);
            }
        } finally {
            this.releaseClient(client);
        }
    }

    private async launchConsumerWithIdleTimeout(
        streamName: string,
        group: string,
        consumerName: string,
        idleTimeout: number = 2000
    ): Promise<void> {
        const client = await this.getClient();
        if (!this.activeConsumers.has(group)) {
            this.activeConsumers.set(group, new Map());
        }
        const consumerMap = this.activeConsumers.get(group)!;
        consumerMap.set(consumerName, { busy: false, lastActive: Date.now() });

        try {
            while (true) {
                const consumerInfo = consumerMap.get(consumerName);
                if (!consumerInfo) break;
                consumerInfo.busy = false;
                const responses: any = await (client as any).xreadgroup(
                    'GROUP',
                    group,
                    consumerName,
                    'BLOCK',
                    idleTimeout,
                    'COUNT',
                    1,
                    'STREAMS',
                    streamName,
                    '>'
                );
                if (!responses) {
                    if (Date.now() - consumerInfo.lastActive >= idleTimeout) {
                        logger.debug(`Consumer ${consumerName} del grupo '${group}' inactivo por ${idleTimeout}ms, liberando conexión`);
                        break;
                    }
                    continue;
                }
                consumerInfo.busy = true;
                consumerInfo.lastActive = Date.now();
                const [, messages] = responses[0];
                for (const [id, fields] of messages) {
                    const rawMessage = fields.reduce(
                        (acc: Record<string, string>, cur: string, index: number, arr: string[]) => {
                            if (index % 2 === 0) {
                                acc[cur] = arr[index + 1] ?? '';
                            }
                            return acc;
                        },
                        {} as Record<string, string>
                    );
                    const message = rawMessage as unknown as DataType;
                    if (message.to !== group) {
                        await client.xack(streamName, group, id);
                        continue;
                    }
                    const callback = this.processMap.get(streamName);
                    if (callback) {
                        await callback(message);
                        await client.xack(streamName, group, id);
                    }
                    consumerInfo.lastActive = Date.now();
                    consumerInfo.busy = false;
                }
            }
        } catch (error) {
            logger.error(`Error en consumer ${consumerName} del grupo '${group}':`, error);
        } finally {
            try {
                await client.xgroup('DELCONSUMER', streamName, group, consumerName);
            } catch (err) {
                logger.error(`Error eliminando consumer ${consumerName} del grupo '${group}':`, err);
            }
            consumerMap.delete(consumerName);
            if (consumerMap.size === 0) {
                this.activeConsumers.delete(group);
            }
            this.releaseClient(client);
            logger.debug(`Consumer ${consumerName} del grupo '${group}' finalizado y eliminado de Redis`);
        }
    }

    async add(streamName: string, group: string, data: DataType): Promise<string | null> {
        const client = await this.getClient();
        try {
            try {
                await client.xgroup('CREATE', streamName, group, '$', 'MKSTREAM');
                logger.debug(`Grupo '${group}' creado en el stream '${streamName}'`);
            } catch (error: any) {
                if (error.message.includes('BUSYGROUP')) {
                    logger.debug(`El grupo '${group}' ya existe en el stream '${streamName}'`);
                } else {
                    logger.error('Error al crear el grupo:', error);
                    throw error;
                }
            }

            const nConsumers = this.nConsumersMap.get(streamName) || 1;
            const active = this.activeConsumers.get(group);
            let available = false;
            if (active) {
                for (const [, info] of active) {
                    if (!info.busy) {
                        available = true;
                        break;
                    }
                }
            }
            if (!available) {
                const count = active ? active.size : 0;
                if (count < nConsumers) {
                    const consumerName = `consumer-${count + 1}`;
                    setTimeout(() => {
                        this.launchConsumerWithIdleTimeout(streamName, group, consumerName);
                    }, 0);
                }
            }

            const fields = Object.entries(data).reduce((acc: string[], [key, value]) => {
                acc.push(key, typeof value === 'string' ? value : JSON.stringify(value));
                return acc;
            }, [] as string[]);
            const messageId = await client.xadd(streamName, '*', ...fields);
            logger.debug(`Mensaje agregado al stream '${streamName}' con id ${messageId}`);
            return messageId;
        } catch (error) {
            logger.error('Error al agregar mensaje:', error);
            throw error;
        } finally {
            this.releaseClient(client);
        }
    }

    async process(streamName: string, nConsumers: number, callback: (message: DataType) => Promise<void>): Promise<void> {
        this.processMap.set(streamName, callback);
        this.nConsumersMap.set(streamName, nConsumers);
    }
}