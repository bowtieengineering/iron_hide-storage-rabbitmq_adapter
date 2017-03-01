require 'bunny'
require 'multi_json'
require 'securerandom'
require 'mutex'
require 'condition_variable'

module IronHide
  class Storage
    class RabbitMQAdapter
      class BunnyClient
        attr_reader :reply_queue
        attr_accessor :response, :call_id
        attr_reader :lock, :condition

        def initialize(ch, server_queue)
          @ch = ch
          @x = ch.default_exchange
          @server_queue = server_queue
          @reply_queue = ch.queue("", exclusive: true)

          @lock = Mutex.new
          @condition = ConditionVariable.new
          that = self

          @reply_queue.subscribe do |delivery_info, properties, payload|
            if properties[:correlation_id] == that.call_id
              that.response = MultiJson.load(payload)
              that.lock.synchronize{that.condition.signal}
            end
          end
        end

        def call(payload)
          self.call_id = SecureRandom.uuid
          @x.publish(payload,
                    routing_key: @server_queue,
                    correlation_id: call_id,
                    reply_to: @reply_queue.name)
          lock.synchronize{condition.wait(lock, 5)}
          response
        end
      end
    end
  end
end
