import Redis, { RedisOptions } from 'ioredis';
import { createPool, Pool } from 'generic-pool';

/**
 * Crea un pool de conexiones Redis
 * @param credentials Opciones de configuración de Redis
 * @returns Pool de conexiones Redis
 */
export const createRedisPool = (credentials: RedisOptions): Pool<Redis> => {
  return createPool<Redis>(
    {
      create: async () => new Redis(credentials),
      destroy: async (client) => {
        await client.quit();
      }, // Esto asegura que devuelva void
    },
    {
      max: 1000, // Máximo de conexiones simultáneas
      min: 5,    // Mínimo de conexiones activas
    }
  );
};