-- enqueue.lua
local queue_groups_key = KEYS[1]
local group_queue_key = KEYS[2]
local job_data = ARGV[1]
local group_name = ARGV[2]  -- Ahora recibimos groupName como ARGV[2]

-- Generar un ID único para el trabajo
local job_id = redis.call('INCR', 'qube:queue:job_id')

-- Almacenar los datos del trabajo y groupName en un hash
redis.call('HMSET', 'qube:queue:job:' .. job_id, 'data', job_data, 'status', 'queued', 'groupName', group_name)

-- Añadir el ID del trabajo a la cola del grupo
redis.call('RPUSH', group_queue_key, job_id)

-- Añadir el grupo a la lista de grupos de la cola
redis.call('SADD', queue_groups_key, group_queue_key)

return tostring(job_id)