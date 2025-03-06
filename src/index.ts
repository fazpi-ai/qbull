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

        const queue_subscriber = new Queue({
            credentials: redisConfig,
            type: 'subscriber',
            consumerLimits: {
                QUEUE_MULTI_INSTANCE: 1
            },
            logLevel: 'debug'
        });

        await queue_publisher.init();
        await queue_subscriber.init();

        // Agregamos un trabajo a la cola
        await queue_publisher.add('WHATSAPP', '573205104418', {
            message: 'Hola, ¿cómo estás? 1'
        });

        await queue_publisher.add('WHATSAPP', '573205104418', {
            message: 'Hola, ¿cómo estás? 2'
        });

        await queue_publisher.add('WHATSAPP', '573205104418', {
            message: 'Hola, ¿cómo estás? 3'
        });

        // Nos suscribimos a los eventos de esta instancia
        queue_subscriber.process('WHATSAPP', (job, done) => {
            console.log("😎 - JOBS:")
            console.log(job)
            done();
        })

        // Cerrar la cola al salir del proceso
        process.on('SIGINT', async () => {
            console.log('\n🛑 Cerrando la cola...');
            await queue_publisher.close();
            await queue_subscriber.close();
            process.exit(0);
        });

    } catch (error) {
        console.log("error:")
        console.log(error)
    }



})()