# kafkastream-leaderboard
### Implementing leaderboard pattern with KafkaStream
## Start Kafka with a single node and Zookeper and Redis
```
 docker-compose -f kafka-redis.yml up -d
```
## Stop Kafka single node and Zookeper and Redis
```
 docker-compose -f kafka-redis.yml down
```
## Execute a shell in Kafka Container
```
 docker exec -it kafka-broker bash
```
## Change directory to the Kafka Scripts
```
 cd /opt/bitnami/kafka/bin
```
## Create a Topic with 1 partition and one replication factor
```
 ./kafka-topics.sh \
   --bootstrap-server localhost:9092 \
   --create \
   --topic streaming.leaderboardpattern.input \
   --partitions 1 \
   --replication-factor 1
```
## List Topics
```
 ./kafka-topics.sh \
   --bootstrap-server localhost:9092 \
   --list
```
