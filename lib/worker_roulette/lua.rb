module WorkerRoulette
  module Lua
    @cache = Hash.new

    def self.call(lua_script, keys_accessed = [], args = [], &callback)
      WorkerRoulette.tradesman_connection_pool.with do |redis|
        results = redis.evalsha(sha(lua_script), keys_accessed.length, *keys_accessed, *args)
        results.callback &callback
        results.errback {self.eval(redis, lua_script, keys_accessed, args, &callback)}
      end
    end

    def self.sha(lua_script)
      @cache[lua_script] ||= Digest::SHA1.hexdigest(lua_script)
    end

    def self.cache
      @cache.dup
    end

    def self.clear_cache!
      @cache = {}
    end

    def self.eval(redis, lua_script, keys_accessed, args, &callback)
      results = redis.eval(lua_script, keys_accessed.size, *keys_accessed, *args)
      results.callback &callback
      results.errback  {|err_msg| raise EM::Hiredis::RedisError.new(err_msg)}
    end
  end
end
