module Kafka
  class Consumer < Client
    ERRLEN = 128
    def initialize(conf : Hash(String, String))
      super(conf, LibKafkaC::TYPE_CONSUMER)
      @running = true
      
      # @queue  = LibKafkaC.queue_get_consumer(@handle)
      # @queue = LibKafkaC.queue_get_main(@handle) if @queue.null?
    end

    # def poll_queue(timeout)
    #   rkev = LibKafkaC.queue_poll(@queue, timeout)
    #   evtype = LibKafkaC.event_type(rkev)
    #   case evtype
    #   when LibKafkaC::Event::NONE
    #   when LibKafkaC::Event::FETCH
    #     msg_ptr = LibKafkaC.event_message_next(rkev)
    #     msg = msg_ptr.value
    #     msg.timestamp = LibKafkaC.message_timestamp(msg_ptr, out ts_type)
    #     msg
    #   when LibKafkaC::Event::REBALANCE
    #     err = LibKafkaC.event_error(rkev)
    #     puts "INFO: Rebalance #{err}"
    #   when LibKafkaC::Event::OFFSET_COMMIT
    #     puts "INFO: Offset commit"
    #     err = LibKafkaC.event_error(rkev)
    #     offsets = LibKafkaC.event_topic_partition_list(rkev)
    #     puts err, offsets
    #   when LibKafkaC::Event::ERROR
    #     err = LibKafkaC.event_error(rkev)
    #     puts "ERROR: [#{err}] #{String.new(LibKafkaC.event_error_string(rkev))}" unless err == -191
    #   else
    #     puts "Unknown type #{evtype}"
    #   end
    # end

    def subscribe(topics : Array(String))
      # Create topic partition list with topics and no partition set
      tpl = LibKafkaC.topic_partition_list_new(topics.size)
      topics.each do |topic|
        LibKafkaC.topic_partition_list_add(tpl, topic, -1)
      end
      # Subscribe to topic partition list and check this was successful
      err = LibKafkaC.subscribe(@handle, tpl)
      if err != 0
        raise KafkaConsumerException.new(err) if err != LibKafkaC::OK
      end
      LibKafkaC.topic_partition_list_destroy(tpl)
    end

    def poll(timeout_ms : Int32) : Message?
      message_ptr = LibKafkaC.consumer_poll(@handle, timeout_ms)
      return unless message_ptr
      cMsg = message_ptr.try do |msg|
        Message.new msg.value
      end
      LibKafkaC.message_destroy(message_ptr)
      cMsg
    end

    def each(timeout = 250)
      loop do
        resp = poll(timeout)
        next if resp.nil?
        raise KafkaConsumerException.new(resp.err) if resp.err != LibKafkaC::OK
        yield resp
        break unless @running
      end
    end

    def close()
      @running = false
      LibKafkaC.consumer_close(@handle)
    end

    def finalize()
      LibKafkaC.kafka_destroy(@handle) if @handle
    end
  end
end
