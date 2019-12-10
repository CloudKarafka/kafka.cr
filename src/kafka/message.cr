require "./lib_rdkafka.cr"


module Kafka

  class TopicPartition
    
  end

  class Message
    def initialize(@msg : LibKafkaC::Message)
    end

    def err : Int32
      @msg.err
    end
    
    def payload : Bytes
      Bytes.new(@msg.payload, @msg.len)
    end

    def key : Bytes
      Bytes.new(@msg.key, @msg.key_len)
    end
  end


  class OffsetCommit
    def self.from(ev : LibKafkaC::QueueEvent) : OffsetCommit
      err = LibKafkaC.event_error(ev)
      offsets = LibKafkaC.event_topic_partition_list(ev)
      if err != LibKafkaC::RD_KAFKA_RESP_ERR_NO_ERROR
      else
      end

      err
    end

    def initialize
    end
  end
end

