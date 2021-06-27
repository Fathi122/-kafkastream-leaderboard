package com.kafkaleaderboardpattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaVoteDataGenerator implements Runnable {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String topic = "streaming.leaderboardpattern.input";

    @Override
    public void run() {
        try {
            System.out.println("Starting Kafka Gaming Generator..");
            //Wait for the main flow to be setup.
            Thread.sleep(5000);

            //Setup Kafka Client
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "localhost:9092");

            kafkaProps.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> myProducer
                    = new KafkaProducer<String, String>(kafkaProps);

            //List of dummy candidates
            List<String> candidates = new ArrayList<String>();
            candidates.add("John");
            candidates.add("Freddy");
            candidates.add("Jane");
            candidates.add("Bobby");
            candidates.add("Sana");

            //Define a random number generator
            Random random = new Random();

            //Generate 100 sample Votes produce events
            for (int i = 0; i < 100; i++) {
                //Generate a random candidate & score
                String candidate = candidates.get(random.nextInt(candidates.size()));
                int score = random.nextInt(10) + 1;

                //Use candidate as key. Each candidate will go to the same partition
                String recKey = String.valueOf(candidate);
                String value = String.valueOf(score);

                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(
                                topic,
                                recKey,
                                value);
                // synchronous
                RecordMetadata rmd = myProducer.send(record).get();

                System.out.println(ANSI_PURPLE +
                        "Kafka Voting Stream Generator : Sending Event : "
                        + recKey + " = " + value + ANSI_RESET);

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
