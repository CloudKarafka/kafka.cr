require "./lib_rdkafka.cr"
require "./config.cr"

module Kafka
  class Producer < Client
    # creates a new kafka handle using provided config.
    # Throws exception on error
    def initialize(conf : Hash(String, String))
      super(conf, LibKafkaC::TYPE_PRODUCER)
      @polling = false
      @keep_running = true

      cb = ->(h : LibKafkaC::KafkaHandle, x : Void*, y : Void*) {
        puts "CB #{x}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
      }

      puts "SETS CB"
      LibKafkaC.conf_set_dr_msg_cb(@conf, cb)

    end

    def produce_simple(topic : String, key : Array(UInt8), msg : Array(UInt8))
      rkt = LibKafkaC.topic_new(@handle, topic, nil)
      part = LibKafkaC::PARTITION_UNASSIGNED
      flags = LibKafkaC::MSG_FLAG_COPY
      err = LibKafkaC.produce(rkt, part, flags, msg, msg.size,
                              key, key.size, nil)
      raise KafkaProducerException.new(err, String.new(LibKafkaC.err2str(err))) if err != LibKafkaC::OK
      LibKafkaC.topic_destroy(rkt)
    end

    def produce_batch(topic : String, batch : Array({key: Array(UInt8), msg: Array(UInt8)}))
      rkt = LibKafkaC.topic_new(@handle, topic, nil)
      part = LibKafkaC::PARTITION_UNASSIGNED
      flags = LibKafkaC::MSG_FLAG_COPY
      batch.each do |t|
        err = LibKafkaC.produce(rkt, part, flags,
                                t[:msg], t[:msg].size,
                                t[:key], t[:key].size,
                                nil)
        raise KafkaProducerException.new(err, String.new(LibKafkaC.err2str(err))) if err != LibKafkaC::OK
      end
      LibKafkaC.poll(@handle, 500)
      LibKafkaC.topic_destroy(rkt)
    end

    def poll(timeout = 500)
      LibKafkaC.poll(@handle, timeout)
    end

    private def start_polling
      return if @polling
      spawn do
        @polling = true
        while @keep_running
          puts "poll"
          LibKafkaC.poll(@handle, 500)
          sleep 1000
        end
        @polling = false
      end
    end

    def flush(timeout = 1000)
      @keep_running = false
      LibKafkaC.flush(@handle, timeout)
    end
  end
end
