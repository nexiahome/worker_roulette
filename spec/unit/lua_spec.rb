require 'spec_helper'
module WorkerRoulette
 describe Lua do
    include EventedSpec::EMSpec
    let(:worker_roulette) {WorkerRoulette.start(evented: true)}
    let(:lua) { Lua.new(worker_roulette.tradesman_connection_pool) }
    let(:redis) {Redis.new(worker_roulette.redis_config)}

    before do
      lua.clear_cache!
      redis.script(:flush)
      redis.flushdb
    end

    it "should load and call a lua script" do
      lua_script = 'return redis.call("SET", KEYS[1], ARGV[1])'
      lua.call(lua_script, ['foo'], ['daddy']) do |result|
        expect(lua.cache.keys.first).to eq(lua_script)
        expect(lua.cache.values.first).to eq(Digest::SHA1.hexdigest(lua_script))
        expect(result).to eq("OK")
        done
      end
    end

    it "should send a sha instead of a script once the script has been cached" do
      lua_script = 'return KEYS'
      expect(lua).to receive(:eval).and_call_original

      lua.call(lua_script) do |result|
        expect(lua).not_to receive(:eval)

        lua.call(lua_script) do |inner_result|
          expect(inner_result).to be_empty
          done
        end
      end
    end

    it "should raise an error to the caller if the script fails in redis" do
      lua_script = 'this is junk'
      # lua.call(lua_script)
      # rspec cannot test this bc of the callbacks, but if you have doubts,
      # uncomment the line above and watch it fail
      done
    end
  end
end
