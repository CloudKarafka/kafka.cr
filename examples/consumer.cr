require "../src/kafka"

conf = {#"debug" => "broker",
  "bootstrap.servers" => "localhost:9092",
  "group.id" => "mange3"}

consumer = Kafka::Consumer.new(conf)

begin
  consumer.subscribe(["yeyeyt"])
rescue e 
  puts e.message
end
consumer.each(2000) do |m|
  puts String.new(m.payload)
end
consumer.close
