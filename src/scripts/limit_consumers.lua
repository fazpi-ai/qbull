-- limit_consumers.lua (optimizado)
local consumerSetKey = KEYS[1]  -- Clave del set de consumidores activos
local consumerTTLKey = KEYS[2]  -- Clave del hash para TTLs
local maxConsumers = tonumber(ARGV[1])
local workerId = ARGV[2]
local ttl = tonumber(ARGV[3])

-- Contar consumidores activos de forma eficiente
local activeConsumers = redis.call('SCARD', consumerSetKey)

-- Verificar el número máximo de consumidores
if activeConsumers >= maxConsumers then
    return 0 -- No crear más consumidores
else
    -- Registrar nuevo consumidor
    local consumerKey = workerId
    redis.call('SADD', consumerSetKey, consumerKey)
    redis.call('HSET', consumerTTLKey, consumerKey, ttl)
    redis.call('EXPIRE', consumerTTLKey, ttl)  -- Expira el hash con TTL global

    return 1 -- Consumidor creado exitosamente
end