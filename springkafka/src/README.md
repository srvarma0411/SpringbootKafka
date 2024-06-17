Running the Application
Start Kafka:

Start Zookeeper:
bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka:
bin/kafka-server-start.sh config/server.properties

Create Topics:
bin/kafka-topics.sh --create --topic TOPIC_A --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic TOPIC_B --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic TOPIC_C --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

After the above steps please run SpringkafkaApplication class