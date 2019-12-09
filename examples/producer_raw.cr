require "../src/kafka/lib_rdkafka"

conf_handle = LibKafkaC.conf_new
LibKafkaC.conf_set(conf_handle, "debug", "all", out errStr1, 512)
LibKafkaC.conf_set(conf_handle, "bootstrap.servers", "localhost:9092", out errStr, 512)
handle = LibKafkaC.kafka_new(
  LibKafkaC::TYPE_PRODUCER,
  conf_handle, out pErrStr, 512)
if handle.not_nil!.address == 0_u64
  puts "Unable to create client"
  exit 1
end

# brokers = if ARGV.empty?
#             "localhost:9092"
#           else
#             ARGV.join(",")
#           end

# err = LibKafkaC.brokers_add(handle, brokers)
# if err != LibKafkaC::OK
#   puts "brokers_add: #{String.new(LibKafkaC.err2str(err))}"
#   exit 1
# end

topic = LibKafkaC.topic_new(handle, "crystal-test", nil)


err = LibKafkaC.produce(
  topic,
  -1,
  LibKafkaC::MSG_FLAG_COPY,
  "My test message",
  15,
  "test",
  4,
  nil)
if err != LibKafkaC::OK
  puts "produce: #{String.new(LibKafkaC.err2str(err))}"
end
LibKafkaC.flush(handle, 500)
LibKafkaC.topic_destroy(topic)
LibKafkaC.kafka_destroy(handle)

