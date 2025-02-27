-- get_status.lua
local job_id = ARGV[1]

-- Obtener el estado del trabajo
local status = redis.call('HGET', 'qube:queue:job:' .. job_id, 'status')

return status