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

    const queue = new Queue(redisConfig, 'debug');

    await queue.init();

    // Definir el procesamiento de trabajos
    queue.process('WHATSAPP', 1, async (job: IJobData, done) => {
        console.log(`📥 Procesando trabajo ID: ${job.id}, Datos:`, job.data);

        await job.progress(50);
        await delay(2, 10); // Retraso aleatorio entre 2 y 5 segundos
        await job.progress(100);

        done();
    });

    for (let i = 0; i < 10; i++) {
        queue.add('WHATSAPP', '573205104418', {
            to: '573205104418',
            content: `Este es un mensaje de prueba ${i}`
        })
    }

    // Cerrar la cola al salir del proceso
    process.on('SIGINT', async () => {
        console.log('\n🛑 Cerrando la cola...');
        await queue.close();
        process.exit(0);
    });

})();