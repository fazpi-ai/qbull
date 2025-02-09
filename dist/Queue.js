"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ioredis_1 = require("ioredis");
const generic_pool_1 = require("generic-pool");
const pino_1 = require("pino");
const logger = (0, pino_1.default)({ level: 'debug' });
class Queue {
    constructor(credentials) {
        this.credentials = credentials;
        this.processMap = new Map();
        this.nConsumersMap = new Map();
        this.activeConsumers = new Map();
        const factory = {
            create: async () => {
                const client = new ioredis_1.default({
                    host: this.credentials.host,
                    port: this.credentials.port,
                    password: this.credentials.password,
                    db: this.credentials.db,
                });
                client.on('connect', () => logger.debug('Conexión a Redis establecida'));
                return client;
            },
            destroy: async (client) => {
                client.disconnect();
                logger.debug('Conexión a Redis cerrada');
            },
        };
        this.pool = (0, generic_pool_1.createPool)(factory, { max: 10, min: 2 });
        logger.debug('Pool de conexiones creado');
    }
    async getClient() {
        logger.debug('Adquiriendo cliente de Redis');
        return this.pool.acquire();
    }
    releaseClient(client) {
        this.pool.release(client);
        logger.debug('Cliente de Redis liberado');
    }
    async initStream(streamName, group) {
        const client = await this.getClient();
        try {
            await client.xgroup('CREATE', streamName, group, '$', 'MKSTREAM');
            logger.debug(`Grupo '${group}' creado en el stream '${streamName}'`);
        }
        catch (error) {
            if (!error.message.includes('BUSYGROUP')) {
                logger.error('Error al crear el grupo:', error);
                throw error;
            }
            else {
                logger.debug(`El grupo '${group}' ya existe en el stream '${streamName}'`);
            }
        }
        finally {
            this.releaseClient(client);
        }
    }
    async launchConsumerWithIdleTimeout(streamName, group, consumerName, idleTimeout = 2000) {
        const client = await this.getClient();
        if (!this.activeConsumers.has(group)) {
            this.activeConsumers.set(group, new Map());
        }
        const consumerMap = this.activeConsumers.get(group);
        consumerMap.set(consumerName, { busy: false, lastActive: Date.now() });
        try {
            while (true) {
                const consumerInfo = consumerMap.get(consumerName);
                if (!consumerInfo)
                    break;
                consumerInfo.busy = false;
                const responses = await client.xreadgroup('GROUP', group, consumerName, 'BLOCK', idleTimeout, 'COUNT', 1, 'STREAMS', streamName, '>');
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
                    const rawMessage = fields.reduce((acc, cur, index, arr) => {
                        var _a;
                        if (index % 2 === 0) {
                            acc[cur] = (_a = arr[index + 1]) !== null && _a !== void 0 ? _a : '';
                        }
                        return acc;
                    }, {});
                    const message = rawMessage;
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
        }
        catch (error) {
            logger.error(`Error en consumer ${consumerName} del grupo '${group}':`, error);
        }
        finally {
            try {
                await client.xgroup('DELCONSUMER', streamName, group, consumerName);
            }
            catch (err) {
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
    async add(streamName, group, data) {
        const client = await this.getClient();
        try {
            try {
                await client.xgroup('CREATE', streamName, group, '$', 'MKSTREAM');
                logger.debug(`Grupo '${group}' creado en el stream '${streamName}'`);
            }
            catch (error) {
                if (error.message.includes('BUSYGROUP')) {
                    logger.debug(`El grupo '${group}' ya existe en el stream '${streamName}'`);
                }
                else {
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
            const fields = Object.entries(data).reduce((acc, [key, value]) => {
                acc.push(key, typeof value === 'string' ? value : JSON.stringify(value));
                return acc;
            }, []);
            const messageId = await client.xadd(streamName, '*', ...fields);
            logger.debug(`Mensaje agregado al stream '${streamName}' con id ${messageId}`);
            return messageId;
        }
        catch (error) {
            logger.error('Error al agregar mensaje:', error);
            throw error;
        }
        finally {
            this.releaseClient(client);
        }
    }
    async process(streamName, nConsumers, callback) {
        this.processMap.set(streamName, callback);
        this.nConsumersMap.set(streamName, nConsumers);
    }
}
exports.default = Queue;
