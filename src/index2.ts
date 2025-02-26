import { Queue } from './core/Queue';
import type { RedisOptions } from 'ioredis';

import { IJobData } from './interfaces/queue.interface';

// Configuración de Redis
const redisConfig: RedisOptions = {
    host: '127.0.0.1',
    port: 6379,
};

const delay = (min: number, max: number) => {
    return new Promise(resolve => {
        const time = Math.floor(Math.random() * (max - min + 1) + min) * 1000;
        setTimeout(resolve, time);
    });
};

// Inicializar la cola
(async () => {
    console.log("🚀 Iniciando el sistema de colas...");

    // Cuando se reciba un trabajo, validar si existen consumidores para esa cola y grupo, si no cuenta con un consumidor lo levanta teniendo en cuenta el numero maximos permitidos.
    const queue = new Queue({
        credentials: redisConfig,
        consumerLimits: {
            QUEUE_MULTI_INSTANCE: 1
        },
        logLevel: 'silent'
    });

    const queue2 = new Queue({
        credentials: redisConfig,
        consumerLimits: {
            QUEUE_MULTI_INSTANCE: 1
        },
        logLevel: 'silent'
    });

    await queue.init();

    await queue.process('QUEUE_MULTI_INSTANCE', 1, async (job: IJobData, done) => {
        console.log("😎 JOBS 1:")
        console.log(job)

        await delay(10, 40);

        console.log("-")

        done();
    });

    await queue2.process('QUEUE_MULTI_INSTANCE', 1, async (job: IJobData, done) => {
        console.log("😎 JOBS 2:")
        console.log(job)

        await delay(10, 40);

        console.log("-")

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

})();