module WorkerRoulette
  module Lua
    # @cache = Hash.new { |h, cmd| h[cmd] = AForeman. }

    def self.call(command, *args)
      begin
        redis.evalsha(sha(command), *args)
      rescue RuntimeError
        redis.eval(@cache[command], *args)
      end
    end

    def self.sha(command)
      Digest::SHA1.hexdigest(@cache[command])
    end
  end
end