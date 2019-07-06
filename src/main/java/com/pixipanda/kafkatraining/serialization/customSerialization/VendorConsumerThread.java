package com.pixipanda.kafkatraining.serialization.customSerialization;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by kafka on 30/1/19.
 */
public class VendorConsumerThread implements  Runnable {

    private final KafkaConsumer<String, Vendor> consumer;
    private final String topic;

    public VendorConsumerThread(String brokers, String groupId, String topic) {
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "VendorDeserializer");
        return props;
    }


    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, Vendor> records = consumer.poll(100);
            for (ConsumerRecord<String, Vendor> record : records) {
                Vendor vendor = record.value();
                System.out.println("Receive message: " + vendor + ", Partition: "
                        + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
                        + Thread.currentThread().getId());
            }
        }
    }


    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Please provide command line arguments: groupID & topic");
            System.exit(-1);
        }
        String groupID = args[0];
        String topic = args[1];

        VendorConsumerThread consumer = new VendorConsumerThread("localhost:9092", groupID, topic);
        Thread t1 = new Thread(consumer);
        t1.start();
    }
}
