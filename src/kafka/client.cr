require "./lib_rdkafka.cr"

module Kafka
  class Client
    getter handle
    def initialize(config : Hash(String, String), client_type : Int32)
      @conf = LibKafkaC.conf_new
      config.each do |k, v|
        res = LibKafkaC.conf_set(@conf, k, v, out err, 128)
      end
      @handle = LibKafkaC.kafka_new(client_type, @conf, out errstr, 512)
      raise "Kafka: Unable to create new producer: #{errstr}" if @handle == 0_u64
    end

    def finalize()
      # LibKafkaC.topic_destroy(@topic) if @topic
      LibKafkaC.kafka_destroy(@handle) if @handle
    end
  end
end
