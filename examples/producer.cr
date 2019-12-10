require "../src/kafka"
require "../src/kafka/lib_rdkafka"

conf = { "bootstrap.servers" => "localhost:9092"}
producer = Kafka::Producer.new(conf)
c = 0
while true
  producer.produce("yeyeyt", "key-#{c}".bytes, "Some message #{c}".bytes)
  c += 1
end
producer.flush

