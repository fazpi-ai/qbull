import type { RedisOptions } from 'ioredis';

import { Queue } from './Queue';

import { IJobData } from './interfaces/queue.interface';

(async () => {

    try {
        const redisConfig: RedisOptions = {
            host: '127.0.0.1',
            port: 6379,
        };


        const queue = new Queue({
            credentials: redisConfig,
            consumerLimits: {
                QUEUE_MULTI_INSTANCE: 1
            },
            logLevel: 'debug'
        });

        await queue.init();

        await queue.process('QUEUE_MULTI_INSTANCE', async (job: IJobData, done) => {
            console.log("😎 JOBS:")
            console.log(job)
            done();
        });

        await queue.add('QUEUE_MULTI_INSTANCE', 'group-1', {
            to: `57320510441`,
            content: `Trabajo desde group-1`,
        });

        await queue.add('QUEUE_MULTI_INSTANCE', 'group-2', {
            to: `57320510441`,
            content: `Trabajo desde group-2 1`,
        });

        await queue.add('QUEUE_MULTI_INSTANCE', 'group-2', {
            to: `57320510441`,
            content: `Trabajo desde group-2 2`,
        });

        // Cerrar la cola al salir del proceso
        process.on('SIGINT', async () => {
            console.log('\n🛑 Cerrando la cola...');
            await queue.close();
            process.exit(0);
        });
    } catch (error) {
        console.log("error:") 
        console.log(error)
    }



})()