require "spec_helper"
module WorkerRoulette
  describe "Evented Read Lock" do
    include EventedSpec::EMSpec

    let(:redis)                    { Redis.new(WorkerRoulette.start.redis_config) }
    let(:sender)                   { "katie_80" }
    let(:work_orders)              { "hellot" }
    let(:lock_key)                 { "L*:#{sender}" }
    let(:queued_at)                { 1234567 }
    let(:default_headers)          { Hash["headers" => { "sender" => sender, "queued_at" => (queued_at.to_f * 1_000_000).to_i }] }
    let(:work_orders_with_headers) { default_headers.merge({ "payload" => work_orders }) }
    let(:worker_roulette)          { WorkerRoulette.start(evented: true, host: '127.0.0.1') }
    let(:foreman1)                 { worker_roulette.foreman(sender) }
    let(:foreman2)                 { worker_roulette.foreman("foreman2") }
    let(:tradesman2 )              { worker_roulette.tradesman }
    let(:lua)                      { Lua.new(worker_roulette.tradesman_connection_pool) }

    subject(:tradesman) {worker_roulette.tradesman}

    em_before do
      allow(Time).to receive(:now).and_return(queued_at)
      lua.clear_cache!
      redis.script(:flush)
      redis.flushdb
    end

    it "should lock a queue when it reads from it" do
      evented_readlock_preconditions do
        expect(redis.get(lock_key)).not_to be_nil
        done
      end
    end

    it "should set the lock to expire in 3 seconds" do
      evented_readlock_preconditions do
        expect(redis.ttl(lock_key)).to eq(3)
        done
      end
    end

    it "should not read a locked queue" do
      evented_readlock_preconditions do
        foreman1.enqueue_work_order(work_orders) do #locked
          tradesman2.work_orders! { |work| expect(work).to be_empty; done}
        end
      end
    end

    it "should read from the first available queue that is not locked" do
      evented_readlock_preconditions do
        foreman1.enqueue_work_order(work_orders) do    #locked
          foreman2.enqueue_work_order(work_orders) do  #unlocked
            tradesman2.work_orders!{|work| expect(work.first["headers"]["sender"]).to eq("foreman2"); done}
          end
        end
      end
    end

    it "should release its last lock when it asks for its next work order from another sender" do
      evented_readlock_preconditions do
        foreman2.enqueue_work_order(work_orders) do #unlocked
          expect(tradesman.last_sender).to eq(sender)
          tradesman.work_orders! do |work|
            expect(work.first["headers"]["sender"]).to eq("foreman2")
            expect(redis.get(lock_key)).to be_nil
            done
          end
        end
      end
    end

    it "should not release its lock when it asks for its next work order from the same sender" do
      evented_readlock_preconditions do
        foreman1.enqueue_work_order(work_orders) do #locked
          tradesman.work_orders! do |work|
            expect(tradesman.last_sender).to eq(sender)
            expect(redis.get(lock_key)).not_to be_nil
            done
          end
        end
      end
    end

    it "should not take out another lock if there is no work to do" do
      evented_readlock_preconditions do
        foreman1.enqueue_work_order(work_orders) do #locked
          tradesman.work_orders! do |work_order|
            tradesman.work_orders! do |work|
              expect(work).to be_empty
              expect(redis.get(lock_key)).to be_nil
              done
            end
          end
        end
      end
    end

    def evented_readlock_preconditions(&spec_block)
      foreman1.enqueue_work_order(work_orders) do
        tradesman.work_orders! do |work|
          spec_block.call
        end
      end
    end
  end
end
