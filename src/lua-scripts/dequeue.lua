-- dequeue.lua
local group_queue_key = KEYS[1]

-- Obtener el ID del siguiente trabajo
local job_id = redis.call('LPOP', group_queue_key)

if job_id then
    -- Actualizar el estado del trabajo a "processing"
    redis.call('HSET', 'qube:queue:job:' .. job_id, 'status', 'processing')
    -- Obtener los datos del trabajo y groupName
    local job_data = redis.call('HGET', 'qube:queue:job:' .. job_id, 'data') or ""
    local group_name = redis.call('HGET', 'qube:queue:job:' .. job_id, 'groupName') or ""
    return {job_id, job_data, group_name}
else
    return nil
end