require 'multi_json'
require 'iron_hide'
require 'iron_hide/storage'
require 'bunny'
require 'securerandom'

module IronHide
  class Storage
    class BunnyClient
      attr_reader :reply_queue
      attr_accessor :response, :call_id
      attr_reader :lock, :condition

      def initialize(ch, server_queue)
        @ch = ch
        @x = ch.default_exchange
        @server_queue = server_queue
        @reply_queue = ch.queue("", exclusive: true)

        @lock = ::Mutex.new
        @condition = ::ConditionVariable.new
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
    class RabbitMQAdapter

      # @option opts [String] :resource *required*
      # @option opts [String] :action *required*
      # @return [Array<Hash>] array of canonical JSON representation of rules
      def where(opts = {})
        # self["#{opts.fetch(:resource)}::#{opts.fetch(:action)}"]
        storage_find(opts.fetch(:resource),opts.fetch(:action))
      end

      private
      # Implements an interface that makes selecting rules look like a Hash:
      # @example
      #   {
      #     'com::test::TestResource::read' => {
      #       ...
      #     }
      #   }
      #  adapter['com::test::TestResource::read']
      #  #=> [Array<Hash>]
      #
      # @param [Symbol] val
      # @return [Array<Hash>] array of canonical JSON representation of rules
      def storage_find(resource,action)
        payload = MultiJson.dump({resource: resource, action: action})
        response = bunny_db_rules(payload)
        if !response.empty?
          # MultiJson.load(response)
          response.reduce([]) do |rval, row|
            rval << row["data"]
          end
        else
          []
          # Do Something
        end
      end

      def bunny_db_rules(val)
          # "#{server}/#{database}/_design/rules/_view/resource_rules?key=\"#{val}\""

        conn = ::Bunny.new(automatically_recover: false, hostname: server)
        conn.start
        ch = conn.create_channel
        client = BunnyClient.new(ch, 'authenticatable_rpc_queue')
        response = client.call(val)
        ch.close
        conn.close
        response
      end

      def server
        IronHide.configuration.message_broker
      end

      # def database
      #   IronHide.configuration.couchdb_database
      # end

    end
  end
end

# Add adapter class to IronHide::Storage
IronHide::Storage::ADAPTERS.merge!(rabbitmq: :RabbitMQAdapter)

# Add default configuration variables
IronHide.configuration.add_configuration(message_broker: 'rabbit-main')
