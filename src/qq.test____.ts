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
    let queue: Queue;

    beforeAll(async () => {
        queue = new Queue(redisConfig, 'silent');
        await queue.init();
    }, 10000); // Extender el tiempo de espera para la inicialización

    afterAll(async () => {
        queue.stopMonitor(); // Detener la tarea periódica antes de cerrar
        await queue.close();
    });

    test(
        'Debería procesar trabajos con dos consumidores simultáneamente',
        async () => {
            const processSpy = jest.fn(async (job: IJobData, done) => {
                await job.progress(50);
                await delay(5000); // Simula un procesamiento de 5 segundos
                await job.progress(100);
                done();
            });

            await queue.process('TEST_QUEUE', 2, processSpy);

            // Agregar trabajos
            for (let i = 0; i < 4; i++) {
                await queue.add('TEST_QUEUE', 'test-group', {
                    to: `57320510441${i}`,
                    content: `Mensaje de prueba ${i}`,
                });
            }

            // Esperar suficiente tiempo para procesar los trabajos
            await delay(20000); // Incrementar el tiempo para asegurar que los procesos terminen

            // Detener la tarea periódica manualmente antes de las verificaciones
            queue.stopMonitor();

            // Verificar que se procesaron exactamente los 4 trabajos
            expect(processSpy).toHaveBeenCalledTimes(4);
        },
        30000 // Aumentar el timeout de la prueba a 30 segundos
    );

    test(
        'Debería procesar 4 trabajos en orden con un solo consumidor',
        async () => {
            const processOrder: string[] = [];

            // Espía que registra el orden de los trabajos procesados
            const processSpy = jest.fn(async (job: IJobData, done) => {
                const startTime = new Date().toISOString();
                console.log(`🟢 [${startTime}] Iniciando trabajo: ${job.data.content}`);

                processOrder.push(job.data.content); // Registrar el contenido del trabajo procesado

                await job.progress(50);
                await delay(2000); // Simula un procesamiento de 2 segundos
                await job.progress(100);

                const endTime = new Date().toISOString();
                console.log(`✅ [${endTime}] Trabajo completado: ${job.data.content}`);

                done();
            });

            // Limitar a un solo consumidor
            await queue.process('TEST_QUEUE_SINGLE', 1, processSpy);

            // Agregar 4 trabajos en orden
            for (let i = 0; i < 4; i++) {
                await queue.add('TEST_QUEUE_SINGLE', 'test-group-single', {
                    to: `57320510441${i}`,
                    content: `Mensaje en orden ${i + 1}`,
                });
            }

            // Esperar suficiente tiempo para que los trabajos se procesen
            await delay(10000); // 10 segundos debería ser suficiente para procesar 4 trabajos

            // Detener el monitor para evitar problemas con el cierre
            queue.stopMonitor();

            // ✅ Verificar que los trabajos se procesaron en el orden correcto
            console.log('📝 Orden de procesamiento final:', processOrder);

            expect(processOrder).toEqual([
                'Mensaje en orden 1',
                'Mensaje en orden 2',
                'Mensaje en orden 3',
                'Mensaje en orden 4',
            ]);

            // ✅ Verificar que solo se haya levantado un consumidor
            expect(processSpy).toHaveBeenCalledTimes(4);
        },
        20000 // Aumentar el tiempo máximo de ejecución de la prueba
    );

    test(
        'Debería procesar trabajos solo en la instancia con consumidor definido',
        async () => {
            // 🔄 Creamos dos instancias de Queue
            const queueProcessor = new Queue(redisConfig, 'silent'); // Procesará los trabajos
            const queueAdder = new Queue(redisConfig, 'silent'); // Solo agregará trabajos

            await queueProcessor.init();
            await queueAdder.init();

            const processOrder: string[] = [];
            const processingTimes: number[] = [];

            // Espía para procesar trabajos con tiempo variable
            const processSpy = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🟢 Iniciando trabajo: ${job.data.content}`);

                processOrder.push(job.data.content);

                await job.progress(50);

                // Simula un tiempo de procesamiento aleatorio entre 2 y 6 segundos
                const processingTime = Math.floor(Math.random() * 4000) + 2000;
                await delay(processingTime);

                await job.progress(100);

                const endTime = Date.now();
                const duration = endTime - startTime;
                processingTimes.push(duration);

                console.log(`✅ Trabajo completado: ${job.data.content} en ${duration} ms`);

                done();
            });

            // 🛠️ Definimos el proceso en la primera instancia con 1 consumidor
            await queueProcessor.process('QUEUE_MULTI_INSTANCE', 1, processSpy);

            // ➕ Agregamos 4 trabajos desde la segunda instancia
            for (let i = 0; i < 4; i++) {
                await queueAdder.add('QUEUE_MULTI_INSTANCE', 'group-multi', {
                    to: `57320510441${i}`,
                    content: `Trabajo desde instancia add ${i + 1}`,
                });
            }

            // ⏳ Esperar suficiente tiempo para procesar todos los trabajos
            await delay(20000);

            // 🔒 Detener los monitores antes de cerrar
            queueProcessor.stopMonitor();
            queueAdder.stopMonitor();

            // ✅ Verificar que los trabajos se procesaron en orden
            console.log('📝 Orden de procesamiento final:', processOrder);
            expect(processOrder).toEqual([
                'Trabajo desde instancia add 1',
                'Trabajo desde instancia add 2',
                'Trabajo desde instancia add 3',
                'Trabajo desde instancia add 4',
            ]);

            // ✅ Verificar que solo se haya levantado un consumidor
            expect(processSpy).toHaveBeenCalledTimes(4);

            // ✅ Verificar el tiempo de procesamiento de cada trabajo
            console.log('⏱️ Tiempos de procesamiento por trabajo (en ms):', processingTimes);
            processingTimes.forEach((time) => {
                expect(time).toBeGreaterThanOrEqual(2000);
                expect(time).toBeLessThanOrEqual(6000);
            });

            // 🔐 Cerrar ambas instancias de la cola
            await queueProcessor.close();
            await queueAdder.close();
        },
        30000 // Aumentar el tiempo máximo de ejecución de la prueba
    );

    test(
        'Debería procesar trabajos en una sola instancia por grupo sin procesarlos dos veces',
        async () => {
            // 🔄 Creamos tres instancias de Queue
            const queueProcessor1 = new Queue(redisConfig, 'silent'); // Procesará los trabajos
            const queueProcessor2 = new Queue(redisConfig, 'silent'); // Procesará los trabajos
            const queueAdder = new Queue(redisConfig, 'silent'); // Solo agregará trabajos

            await queueProcessor1.init();
            await queueProcessor2.init();
            await queueAdder.init();

            const processOrderProcessor1: string[] = [];
            const processOrderProcessor2: string[] = [];
            const processingTimes: number[] = [];

            // 📥 Espía para la primera instancia procesadora
            const processSpy1 = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🔵 [Processor 1] Iniciando trabajo: ${job.data.content}`);

                processOrderProcessor1.push(job.data.content);

                await job.progress(50);

                // Simula tiempo aleatorio entre 2 y 4 segundos
                const processingTime = Math.floor(Math.random() * 2000) + 2000;
                await delay(processingTime);

                await job.progress(100);

                const endTime = Date.now();
                const duration = endTime - startTime;
                processingTimes.push(duration);

                console.log(`✅ [Processor 1] Trabajo completado: ${job.data.content} en ${duration} ms`);
                done();
            });

            // 📥 Espía para la segunda instancia procesadora
            const processSpy2 = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🟣 [Processor 2] Iniciando trabajo: ${job.data.content}`);

                processOrderProcessor2.push(job.data.content);

                await job.progress(50);

                // Simula tiempo aleatorio entre 2 y 4 segundos
                const processingTime = Math.floor(Math.random() * 2000) + 2000;
                await delay(processingTime);

                await job.progress(100);

                const endTime = Date.now();
                const duration = endTime - startTime;
                processingTimes.push(duration);

                console.log(`✅ [Processor 2] Trabajo completado: ${job.data.content} en ${duration} ms`);
                done();
            });

            // 🛠️ Definimos el proceso en ambas instancias con 1 consumidor cada una
            await queueProcessor1.process('QUEUE_MULTI_PROCESSORS', 1, processSpy1);
            await queueProcessor2.process('QUEUE_MULTI_PROCESSORS', 1, processSpy2);

            // ➕ Agregamos 6 trabajos desde la tercera instancia
            for (let i = 0; i < 6; i++) {
                await queueAdder.add('QUEUE_MULTI_PROCESSORS', 'group-shared', {
                    to: `57320510441${i}`,
                    content: `Trabajo desde instancia add ${i + 1}`,
                });
            }

            // ⏳ Esperar suficiente tiempo para procesar todos los trabajos
            await delay(30000); // 30 segundos para asegurar la ejecución

            // 🔒 Detener los monitores antes de cerrar
            queueProcessor1.stopMonitor();
            queueProcessor2.stopMonitor();
            queueAdder.stopMonitor();

            // ✅ Verificar que se procesaron los 6 trabajos exactamente una vez
            const allProcessedJobs = [...processOrderProcessor1, ...processOrderProcessor2];
            console.log('📝 Orden de procesamiento final:', allProcessedJobs);

            expect(allProcessedJobs).toHaveLength(6);
            expect(new Set(allProcessedJobs).size).toBe(6); // Verificar que no haya duplicados

            // ✅ Verificar que solo una instancia haya procesado todos los trabajos
            const totalProcessedByInstance = {
                processor1: processOrderProcessor1.length,
                processor2: processOrderProcessor2.length
            };

            console.log('✅ Procesamiento por instancia:', totalProcessedByInstance);

            expect(
                processOrderProcessor1.length === 6 || processOrderProcessor2.length === 6
            ).toBe(true); // Solo una instancia debe haber procesado todos los trabajos

            // ✅ La otra instancia no debe haber procesado ningún trabajo
            expect(
                processOrderProcessor1.length === 0 || processOrderProcessor2.length === 0
            ).toBe(true);

            // ✅ Verificar que cada trabajo se haya procesado en un tiempo válido
            processingTimes.forEach((time) => {
                expect(time).toBeGreaterThanOrEqual(2000);
                expect(time).toBeLessThanOrEqual(4000);
            });

            // 🔐 Cerrar todas las instancias de la cola
            await queueProcessor1.close();
            await queueProcessor2.close();
            await queueAdder.close();
        },
        40000 // Aumentar el tiempo máximo de ejecución de la prueba
    );

    test(
        'Debería procesar trabajos en diferentes colas sin interferencias entre instancias',
        async () => {
            // 🔄 Creamos cinco instancias de Queue
            const queueProcessor1 = new Queue(redisConfig, 'silent'); // Procesará la cola original
            const queueProcessor2 = new Queue(redisConfig, 'silent'); // Procesará la cola original
            const queueProcessor3 = new Queue(redisConfig, 'silent'); // Procesará una nueva cola
            const queueAdderOriginal = new Queue(redisConfig, 'silent'); // Publicará trabajos en la cola original
            const queueAdderNew = new Queue(redisConfig, 'silent'); // Publicará trabajos en la nueva cola

            await queueProcessor1.init();
            await queueProcessor2.init();
            await queueProcessor3.init();
            await queueAdderOriginal.init();
            await queueAdderNew.init();

            const processOrderProcessor1: string[] = [];
            const processOrderProcessor2: string[] = [];
            const processOrderProcessor3: string[] = [];
            const processingTimes: number[] = [];

            // 📥 Espía para la primera instancia procesadora
            const processSpy1 = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🔵 [Processor 1] Iniciando trabajo: ${job.data.content}`);

                processOrderProcessor1.push(job.data.content);
                await job.progress(50);

                // Simula tiempo aleatorio entre 2 y 4 segundos
                const processingTime = Math.floor(Math.random() * 2000) + 2000;
                await delay(processingTime);

                await job.progress(100);

                const endTime = Date.now();
                processingTimes.push(endTime - startTime);
                done();
            });

            // 📥 Espía para la segunda instancia procesadora
            const processSpy2 = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🟣 [Processor 2] Iniciando trabajo: ${job.data.content}`);

                processOrderProcessor2.push(job.data.content);
                await job.progress(50);

                const processingTime = Math.floor(Math.random() * 2000) + 2000;
                await delay(processingTime);

                await job.progress(100);

                const endTime = Date.now();
                processingTimes.push(endTime - startTime);
                done();
            });

            // 📥 Espía para la tercera instancia procesadora (nueva cola)
            const processSpy3 = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🟡 [Processor 3 - Nueva Cola] Iniciando trabajo: ${job.data.content}`);

                processOrderProcessor3.push(job.data.content);
                await job.progress(50);

                const processingTime = Math.floor(Math.random() * 2000) + 2000;
                await delay(processingTime);

                await job.progress(100);

                const endTime = Date.now();
                processingTimes.push(endTime - startTime);
                done();
            });

            // 🛠️ Definimos el proceso en las instancias
            await queueProcessor1.process('QUEUE_MULTI_PROCESSORS', 1, processSpy1);
            await queueProcessor2.process('QUEUE_MULTI_PROCESSORS', 1, processSpy2);
            await queueProcessor3.process('QUEUE_NEW_PROCESSORS', 1, processSpy3);

            // ➕ Agregamos 6 trabajos para la cola original desde la cuarta instancia
            for (let i = 0; i < 6; i++) {
                await queueAdderOriginal.add('QUEUE_MULTI_PROCESSORS', 'group-shared', {
                    to: `57320510441${i}`,
                    content: `Trabajo original ${i + 1}`,
                });
            }

            // ➕ Agregamos 4 trabajos para la nueva cola desde la quinta instancia
            for (let i = 0; i < 4; i++) {
                await queueAdderNew.add('QUEUE_NEW_PROCESSORS', 'group-new', {
                    to: `57320510450${i}`,
                    content: `Trabajo nuevo ${i + 1}`,
                });
            }

            // ⏳ Esperar suficiente tiempo para procesar todos los trabajos
            await delay(40000); // 40 segundos para asegurar la ejecución

            // 🔒 Detener los monitores antes de cerrar
            queueProcessor1.stopMonitor();
            queueProcessor2.stopMonitor();
            queueProcessor3.stopMonitor();
            queueAdderOriginal.stopMonitor();
            queueAdderNew.stopMonitor();

            // ✅ Verificar que los trabajos originales se procesaron exactamente una vez
            const allProcessedOriginalJobs = [
                ...processOrderProcessor1,
                ...processOrderProcessor2
            ];
            console.log('📝 Orden de procesamiento final (original):', allProcessedOriginalJobs);

            expect(allProcessedOriginalJobs).toHaveLength(6);
            expect(new Set(allProcessedOriginalJobs).size).toBe(6); // Verificar duplicados

            // ✅ Verificar que solo una instancia haya procesado los trabajos por grupo (original)
            const totalProcessedByInstanceOriginal = {
                processor1: processOrderProcessor1.length,
                processor2: processOrderProcessor2.length
            };

            console.log('✅ Procesamiento por instancia (original):', totalProcessedByInstanceOriginal);
            expect(
                processOrderProcessor1.length === 6 || processOrderProcessor2.length === 6
            ).toBe(true); // Solo una instancia debe haber procesado los trabajos del grupo original

            // ✅ Verificar que los trabajos de la nueva cola fueron procesados por la instancia correcta
            console.log('📝 Orden de procesamiento final (nueva cola):', processOrderProcessor3);
            expect(processOrderProcessor3).toHaveLength(4); // Todos deben ser procesados por Processor 3

            // ✅ Verificar que cada trabajo se haya procesado en un tiempo válido
            processingTimes.forEach((time) => {
                expect(time).toBeGreaterThanOrEqual(2000);
                expect(time).toBeLessThanOrEqual(4000);
            });

            // 🔐 Cerrar todas las instancias de la cola
            await queueProcessor1.close();
            await queueProcessor2.close();
            await queueProcessor3.close();
            await queueAdderOriginal.close();
            await queueAdderNew.close();
        },
        50000 // Aumentar el tiempo máximo de ejecución de la prueba
    );

    test.only(
        'Debería procesar trabajos en dos instancias separadas por grupo en la misma cola',
        async () => {
            // 🔄 Creamos cuatro instancias de Queue
            const queueProcessor1 = new Queue(redisConfig, 'silent'); // Procesará trabajos de group-one
            const queueProcessor2 = new Queue(redisConfig, 'silent'); // Procesará trabajos de group-two
            const queueAdder1 = new Queue(redisConfig, 'silent'); // Publicará trabajos en group-one
            const queueAdder2 = new Queue(redisConfig, 'silent'); // Publicará trabajos en group-two
    
            await queueProcessor1.init();
            await queueProcessor2.init();
            await queueAdder1.init();
            await queueAdder2.init();
    
            const processOrderProcessor1: string[] = [];
            const processOrderProcessor2: string[] = [];
            const processingTimes: number[] = [];
    
            // 📥 Espía para la primera instancia procesadora (group-one)
            const processSpy1 = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🔵 [Processor 1] Iniciando trabajo (group-one): ${job.data.content}`);
    
                processOrderProcessor1.push(job.data.content);
                await job.progress(50);
    
                const processingTime = Math.floor(Math.random() * 2000) + 2000;
                await delay(processingTime);
    
                await job.progress(100);
    
                const endTime = Date.now();
                processingTimes.push(endTime - startTime);
                done();
            });
    
            // 📥 Espía para la segunda instancia procesadora (group-two)
            const processSpy2 = jest.fn(async (job: IJobData, done) => {
                const startTime = Date.now();
                console.log(`🟣 [Processor 2] Iniciando trabajo (group-two): ${job.data.content}`);
    
                processOrderProcessor2.push(job.data.content);
                await job.progress(50);
    
                const processingTime = Math.floor(Math.random() * 2000) + 2000;
                await delay(processingTime);
    
                await job.progress(100);
    
                const endTime = Date.now();
                processingTimes.push(endTime - startTime);
                done();
            });
    
            // 🛠️ Definimos el proceso en ambas instancias con 1 consumidor cada una
            await queueProcessor1.process('QUEUE_MULTI_PROCESSORS', 2, processSpy1);
            await queueProcessor2.process('QUEUE_MULTI_PROCESSORS', 2, processSpy2);
    
            // ➕ Agregamos 4 trabajos en cada grupo desde las otras dos instancias
            for (let i = 0; i < 4; i++) {
                await queueAdder1.add('QUEUE_MULTI_PROCESSORS', 'group-one', {
                    to: `57320510441${i}`,
                    content: `Trabajo grupo uno ${i + 1}`,
                });
    
                await queueAdder2.add('QUEUE_MULTI_PROCESSORS', 'group-two', {
                    to: `57320510450${i}`,
                    content: `Trabajo grupo dos ${i + 1}`,
                });

                await queueAdder2.add('QUEUE_MULTI_PROCESSORS', 'group-three', {
                    to: `57320510450${i}`,
                    content: `Trabajo grupo tres ${i + 1}`,
                });

                await queueAdder2.add('QUEUE_MULTI_PROCESSORS', 'group-four', {
                    to: `57320510450${i}`,
                    content: `Trabajo grupo cuatro ${i + 1}`,
                });
                
            }
    
            // ⏳ Esperar suficiente tiempo para procesar todos los trabajos
            await delay(30000); // 30 segundos para asegurar la ejecución
    
            // 🔒 Detener los monitores antes de cerrar
            queueProcessor1.stopMonitor();
            queueProcessor2.stopMonitor();
            queueAdder1.stopMonitor();
            queueAdder2.stopMonitor();
    
            // ✅ Verificar que los trabajos del group-one se procesaron en processor1
            console.log('📝 Procesados por Processor 1 (group-one):', processOrderProcessor1);
            expect(processOrderProcessor1).toHaveLength(4);
    
            // ✅ Verificar que los trabajos del group-two se procesaron en processor2
            console.log('📝 Procesados por Processor 2 (group-two):', processOrderProcessor2);
            expect(processOrderProcessor2).toHaveLength(4);
    
            // ✅ Asegurar que los trabajos no se mezclaron entre grupos
            expect(new Set([...processOrderProcessor1, ...processOrderProcessor2]).size).toBe(8);
    
            // ✅ Verificar que cada trabajo se haya procesado en un tiempo válido
            processingTimes.forEach((time) => {
                expect(time).toBeGreaterThanOrEqual(2000);
                expect(time).toBeLessThanOrEqual(4000);
            });
    
            // 🔐 Cerrar todas las instancias de la cola
            await queueProcessor1.close();
            await queueProcessor2.close();
            await queueAdder1.close();
            await queueAdder2.close();
        },
        40000 // Aumentar el tiempo máximo de ejecución de la prueba
    );

});