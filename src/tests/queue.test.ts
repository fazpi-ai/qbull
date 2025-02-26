import { Queue } from '../core/Queue';
import type { RedisOptions } from 'ioredis';
import { IJobData } from '../interfaces/queue.interface';

// Configuración de Redis de prueba
const redisConfig: RedisOptions = {
    host: '127.0.0.1',
    port: 6379,
};

// Mock de delay
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('Queue Tests', () => {

    test(
        'El objetivo es que se levante un consumidor dinamico por cada grupo, en este ejemplo se espera 3 registros STRING en redis con un TTL de 30 segundos, los registros tiene que seguir el esquema qube:queueName:groupName.',
        async () => {

            // Cuando se reciba un trabajo, validar si existen consumidores para esa cola y grupo, si no cuenta con un consumidor lo levanta teniendo en cuenta el numero maximos permitidos.
            const queue = new Queue({
                credentials: redisConfig,
                consumerLimits: {
                    QUEUE_MULTI_INSTANCE: 1
                },
                logLevel: 'debug'
            });

            await queue.init();

            await queue.process('QUEUE_MULTI_INSTANCE', 1, jest.fn(async (job: IJobData, done) => {
                console.log("job:")
                console.log(job)
                done();
            }));

            await queue.add('QUEUE_MULTI_INSTANCE', 'group-1', {
                to: `57320510441`,
                content: `Trabajo desde group-1`,
            });

            /*
            await queue.add('QUEUE_MULTI_INSTANCE', 'group-2', {
                to: `57320510441`,
                content: `Trabajo desde group-1`,
            });

            await queue.add('QUEUE_MULTI_INSTANCE', 'group-3', {
                to: `57320510441`,
                content: `Trabajo desde group-1`,
            });
            */

        })

})