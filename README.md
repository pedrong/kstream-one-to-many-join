# Kafka Streams One-To-Many Join Transformer
Custom Kafka Streams transformer using [Processor API](https://docs.confluent.io/current/streams/developer-guide/processor-api.html)
which allows you to do a join on multiple keys.
The transformer must be initialized with:
* [GlobalKTable](https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/GlobalKTable.html) where the keys will be looked up
* [KeyValueMapper](https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/KeyValueMapper.html) which must return a collection of keys
* [ValueJoiner](https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/ValueJoiner.html) which will receive a list of records found from the lookup/join and must join them to the current record being processed

## Usage
This repo contains an example usage at src/main/java/com/github/pedrong/Main.java

This example creates a topic containing employees that will be used as the GlobalKTable.
It then creates a stream listening for departments on a different topic and joining the multiple employee ids in each department message with the employeeds from the KTable.