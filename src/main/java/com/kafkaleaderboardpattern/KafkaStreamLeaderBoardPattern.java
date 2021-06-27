package com.kafkaleaderboardpattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamLeaderBoardPattern {
    public static void main(String[] args) {
        //Initiate  RedisTracker will print leader positions every 5 seconds
        RedisControl redisTracker = new RedisControl();
        redisTracker.setUp();
        Thread redisThread = new Thread(redisTracker);
        redisThread.start();

        //Initiate RedisUpdater will update the leaderboard in Redis
        RedisControl redisUpdater = new RedisControl();
        redisUpdater.setUp();

        //Initiate the Kafka Voting data Generator
        KafkaVoteDataGenerator votingGenerator = new KafkaVoteDataGenerator();
        Thread genThread = new Thread(votingGenerator);
        genThread.start();
        System.out.println("******** Starting Streaming  *************");

        try {
            /**************************************************
             * Build a Kafka Topology
             **************************************************/

            //Setup Serializer / DeSerializer for used Data types
            final Serde<String> stringSerde = Serdes.String();

            //Setup Properties for the Kafka Input Stream
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                    "leaderboards-pipe");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Only in test environment to avoid waiting
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

            //Initiate the Kafka Streams Builder
            final StreamsBuilder builder = new StreamsBuilder();

            //Create the source node for Voting data
            KStream<String, String> votingInput
                    = builder.stream("streaming.leaderboardpattern.input",
                    Consumed.with(
                            stringSerde, stringSerde));
            // display Candidate score from consumed event on topic streaming.leaderboardpattern.input
            votingInput
                    .peek(new ForeachAction<String, String>() {
                        @Override
                        public void apply(String candidate, String score) {
                            System.out.println(new StringBuilder().append("Received Score : Candidate = ").append(candidate).append(", Score = ").append(score).toString());
                        }
                    });

            //Update the Redis key with the new voting score increment
            votingInput
                    .foreach(new ForeachAction<String, String>() {
                                 @Override
                                 public void apply(String candidate, String score) {
                                     redisUpdater.update_vote_score(candidate, Double.valueOf(score));
                                 }
                             }
                    );

            /**************************************************
             * Create a pipeline and execute it
             **************************************************/
            //Create final topology and print
            final Topology topology = builder.build();
            System.out.println(topology.describe());

            //Setup Stream
            final KafkaStreams streams = new KafkaStreams(topology, props);

            //Reset only in test environment
            streams.cleanUp();
            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    System.out.println("Shutdown called..");
                    streams.close();
                    latch.countDown();
                }
            });
            streams.start();
            // wait for latch to be decremented
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
