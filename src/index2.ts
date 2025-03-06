import type { RedisOptions } from 'ioredis';

import { Queue } from './Queue';

(async () => {

    try {

        const redisConfig: RedisOptions = {
            host: '127.0.0.1',
            port: 6379,
        };

        const queue_publisher = new Queue({
            credentials: redisConfig,
            type: 'publisher',
            logLevel: 'debug'
        });

        await queue_publisher.init();

        await queue_publisher.add('WHATSAPP', '573205104418', {
            message: 'Hola, ¿cómo estás? 4'
        });

        // Cerrar la cola al salir del proceso
        process.on('SIGINT', async () => {
            console.log('\n🛑 Cerrando la cola...');
            await queue_publisher.close();
            process.exit(0);
        });

    } catch (error) {
        console.log("error:")
        console.log(error)
    }



})()