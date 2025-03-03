import type { RedisOptions } from 'ioredis';

import { Queue } from './Queue';

import { IJobData } from './interfaces/queue.interface';

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

        // Nos suscribimos a los eventos de esta instancia
        queue_subscriber.process('WHATSAPP', (job, done) => {
            console.log("😎 JOBS:")
            console.log(job)
            done();
        })

        // Agregamos un trabajo a la cola
        const jobId = await queue_publisher.add('WHATSAPP', '573205104418', {
            message: 'Hola, ¿cómo estás?'
        });

        console.log("😎 JOB ID:")
        console.log(jobId)

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