package com.microsoft.piq.dmp.data;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static int consume(String brokers, String groupId, String topicName) {
        // Create a consumer
        KafkaConsumer<String, String> consumer;
        // configure the consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        // Set the consumer group (all consumers must belong to a group).
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Set how to serialize key/value pairs
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        // When a group is first created, it has no offset stored to start reading from. This tells it to start
        // with the earliest record in the stream.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topicName));

        // Loop until ctrl+c
        int count = 0;
        while (true) {
            // poll for records
            ConsumerRecords<String, String> records = consumer.poll(200);

            if (records.count() == 0) {
                // timeout/nothing to read
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    count += 1;
                    System.out.println(count + ": " + record.value());
                }
            }
        }
    }
}
