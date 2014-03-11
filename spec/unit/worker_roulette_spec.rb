require 'spec_helper'
module WorkerRoulette
  describe WorkerRoulette do
    it "should not explode if redis_config is called before start" do
      WorkerRoulette.instance_variable_set("@redis_config", nil)
      expect {WorkerRoulette.redis_config}.not_to raise_error
    end

    it "should not explode if pool_size is called before start" do
      WorkerRoulette.instance_variable_set("@pool_config", nil)
      expect {WorkerRoulette.pool_size}.not_to raise_error
    end
  end
end
