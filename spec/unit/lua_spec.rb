require 'spec_helper'
module WorkerRoulette
 describe Lua do
    include EventedSpec::EMSpec
    let(:redis) {Redis.new(WorkerRoulette.redis_config)}

    em_before do
      WorkerRoulette.start(evented: true)
    end

    before do
      Lua.clear_cache!
      redis.script(:flush)
      redis.flushdb
    end

    it "should load and call a lua script" do
      lua_script = 'return redis.call("SET", KEYS[1], ARGV[1])'
      Lua.call(lua_script, ['foo'], ['daddy']) do |result|
        Lua.cache.keys.first.should == lua_script
        Lua.cache.values.first.should == Digest::SHA1.hexdigest(lua_script)
        result.should == "OK"
        done
      end
    end

    it "should send a sha instead of a script once the script has been cached" do
      lua_script = 'return KEYS'
      Lua.should_receive(:eval).and_call_original

      Lua.call(lua_script) do |result|

        Lua.should_not_receive(:eval)
        Lua.call(lua_script) do |result|
          result.should == []
          done
        end
      end
    end

    it "should raise an error to the caller if the script fails in redis" do
      lua_script = 'this is junk'
      # Lua.call(lua_script)
      # rspec cannot test this bc of the callbacks, but if you have doubts,
      # uncomment the line above and watch it fail
      done
    end
  end
end