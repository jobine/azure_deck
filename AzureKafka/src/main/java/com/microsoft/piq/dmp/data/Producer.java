package com.microsoft.piq.dmp.data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void produce(String brokers, String topicName) throws IOException {
        // set properties used to configure the producer
        Properties properties = new Properties();
        // set the brokers (boostrap servers)
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        // set ow to serialize key/value pairs
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        Random random = new Random();

        String[] sentences = new String[] {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
        };

        String progressAnimation = "|/-\\";
        // produce a bunch of records
        for (int i = 0; i < 100; i++) {
            // pick a sentence at random
            String sentence = "[" + Instant.now().toString() + "] " + sentences[random.nextInt(sentences.length)];

            // set the sentence to the topic
            try {
                producer.send(new ProducerRecord<String, String>(topicName, sentence)).get();
            } catch (Exception ex) {
                System.out.print(ex.getMessage());
                throw new IOException(ex.toString());
            }

            String progressBar = "\r" + progressAnimation.charAt(i % progressAnimation.length()) + " " + i;
            System.out.write(progressBar.getBytes());
        }
    }
}
