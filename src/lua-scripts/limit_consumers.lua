-- limit_consumers.lua (versión mejorada)
local keyPrefix = KEYS[1]
local maxConsumers = tonumber(ARGV[1])
local workerId = ARGV[2]
local ttl = tonumber(ARGV[3])

-- Contar consumidores activos
local activeConsumers = 0
local cursor = "0"
repeat
    local result = redis.call('SCAN', cursor, 'MATCH', keyPrefix .. ':*', 'COUNT', 100)
    cursor = result[1]
    local keys = result[2]

    for _, key in ipairs(keys) do
        if redis.call('EXISTS', key) == 1 then
            activeConsumers = activeConsumers + 1
        end
    end
until cursor == "0"

-- Verificar el número máximo de consumidores
if activeConsumers >= maxConsumers then
    return 0 -- No crear más consumidores
else
    -- Registrar nuevo consumidor con TTL
    local consumerKey = keyPrefix .. ':' .. workerId
    redis.call('SET', consumerKey, 'active', 'EX', ttl)
    return 1 -- Consumidor creado exitosamente
end