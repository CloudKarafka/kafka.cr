require "../src/kafka"
require "../src/kafka/lib_rdkafka"


conf = {#"debug" => "broker",
                         "bootstrap.servers" => "localhost:9092"}

producer = Kafka::Producer.new(conf)
producer.produce_simple("yeyeyt", "bla".bytes, "bla bla bla".bytes)
producer.poll
producer.produce_simple("yeyeyt", "bla".bytes, "bla bla bla".bytes)
producer.poll
producer.produce_simple("yeyeyt", "bla".bytes, "bla bla bla".bytes)
producer.poll
producer.produce_simple("yeyeyt", "bla".bytes, "bla bla bla".bytes)
producer.poll
producer.produce_simple("yeyeyt", "bla".bytes, "bla bla bla".bytes)
producer.poll
producer.flush

msgs = [
  {key: "123".bytes, msg: "Some msg".bytes},
  {key: "123".bytes, msg: "Some msg".bytes},
  {key: "123".bytes, msg: "Some msg".bytes},
  {key: "123".bytes, msg: "Some msg".bytes},
  {key: "123".bytes, msg: "Some msg".bytes},
  {key: "123".bytes, msg: "Some msg".bytes},
]
producer.produce_batch("yeyeyt", msgs)
producer.flush
#LibKafkaC.kafka_destroy(handle)
