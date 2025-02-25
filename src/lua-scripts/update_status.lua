-- update_status.lua
local job_id = ARGV[1]
local new_status = ARGV[2]

-- Actualizar el estado del trabajo
redis.call('HSET', 'qube:queue:job:' .. job_id, 'status', new_status)

return true