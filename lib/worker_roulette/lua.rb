module WorkerRoulette
  module Lua
    @cache = Hash.new

    def self.call(lua_script, keys_accessed = [], args = [], &callback)
      WorkerRoulette.tradesman_connection_pool.with do |redis|
        results = evalsha(redis, lua_script, keys_accessed, args, &callback)
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
      results = redis.eval(lua_script, keys_accessed.length, *keys_accessed, *args)
      results.callback &callback if callback
      results.errback  {|err_msg| raise EM::Hiredis::RedisError.new(err_msg)}
    end

    def self.evalsha(redis, lua_script, keys_accessed, args, &callback)
      if redis.class == EM::Hiredis::Client
        results = redis.evalsha(sha(lua_script), keys_accessed.length, *keys_accessed, *args)
        results.callback &callback if callback
        results.errback {self.eval(redis, lua_script, keys_accessed, args, &callback)}
      else
        begin
          results = redis.evalsha(sha(lua_script), keys_accessed, args)
        rescue Redis::CommandError
          results = redis.eval(lua_script, keys_accessed, args)
        ensure
          return callback.call results if callback
        end
      end
      results
    end
  end
end
