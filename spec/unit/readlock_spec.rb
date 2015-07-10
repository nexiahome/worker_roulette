require "spec_helper"

module WorkerRoulette
 describe "Read Lock" do
    let(:worker_roulette)          { WorkerRoulette.start(evented: false) }
    let(:redis)                    { Redis.new(worker_roulette.redis_config) }
    let(:sender)                   { "katie_80" }
    let(:work_orders)              { "hello" }
    let(:lock_key)                 { "L*:#{sender}" }
    let(:queued_at)                { 1234567 }
    let(:default_headers)          { Hash["headers" => { "sender" => sender, "queued_at" => (queued_at.to_f * 1_000_000).to_i }] }
    let(:work_orders_with_headers) { default_headers.merge({ "payload" => work_orders }) }
    let(:foreman1)                 { worker_roulette.foreman(sender) }
    let(:foreman2)                 { worker_roulette.foreman("foreman2") }
    let(:lua)                      { Lua.new(worker_roulette.tradesman_connection_pool) }
    let(:tradesman_1)              { worker_roulette.tradesman }
    let(:tradesman_2)              { worker_roulette.tradesman }

    before do
      lua.clear_cache!
      redis.script(:flush)
      redis.flushdb
      allow(Time).to receive(:now).and_return(queued_at)
      foreman1.enqueue_work_order(work_orders)
      expect(tradesman_1.work_orders!).to eq([work_orders_with_headers])
    end

    it "locks a queue when it reads from it" do
      expect(redis.get(lock_key)).not_to be_nil
    end

    it "sets the lock to expire in 3 seconds" do
      expect(redis.ttl(lock_key)).to eq(3)
    end

    it "does not read a locked queue" do
      foreman1.enqueue_work_order(work_orders)    #locked
      expect(tradesman_2.work_orders!).to be_empty
    end

    it "reads from the first available queue that is not locked" do
       foreman1.enqueue_work_order(work_orders)     #locked
       foreman2.enqueue_work_order(work_orders)  #unlocked
       expect(tradesman_2.work_orders!.first["headers"]["sender"]).to eq("foreman2")
    end

    it "releases its previous lock when it asks for work from another sender" do
      foreman2.enqueue_work_order(work_orders)    #unlocked
      expect(tradesman_1.last_sender).to eq(sender)
      expect(tradesman_1.work_orders!.first["headers"]["sender"]).to eq("foreman2")
      expect(redis.get(lock_key)).to be_nil
    end

    it "does not release its lock when it asks for work from the same sender" do
      foreman1.enqueue_work_order(work_orders)    #locked
      expect(tradesman_1.work_orders!).to eq([work_orders_with_headers])
      expect(tradesman_1.last_sender).to eq(sender)

      foreman1.enqueue_work_order(work_orders)    #locked
      expect(tradesman_1.work_orders!).to eq([work_orders_with_headers])
      expect(tradesman_1.last_sender).to eq(sender)

      expect(redis.get(lock_key)).not_to be_nil
    end

    it "releases its previous lock if there is no work to do from the same sender" do
      foreman1.enqueue_work_order(work_orders)    #locked
      expect(tradesman_1.work_orders!).to eq([work_orders_with_headers])
      expect(tradesman_1.work_orders!).to be_empty
      expect(redis.get(lock_key)).to be_nil
    end
  end
end
