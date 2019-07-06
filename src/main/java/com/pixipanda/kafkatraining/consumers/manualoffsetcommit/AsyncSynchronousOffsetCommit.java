package com.pixipanda.kafkatraining.consumers.manualoffsetcommit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by kafka on 21/1/19.
 */
public class AsyncSynchronousOffsetCommit implements  Runnable{

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public AsyncSynchronousOffsetCommit(String brokers, String groupId, String topic) {
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {

        try {

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Receive message: " + record.value() + ", Partition: "
                        + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
                        + Thread.currentThread().getId());
            }
            consumer.commitAsync();
        }

        }catch (Exception e) {
            System.out.println("Unexpected error" + e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Please provide command line arguments: groupID and topic");
            System.exit(-1);
        }

        String brokers = "localhost:9092";
        String groupId = args[0];
        String topic = args[1];
        AsyncSynchronousOffsetCommit consumer = new AsyncSynchronousOffsetCommit("localhost:9092", groupId, topic);
        Thread t1 = new Thread(consumer);
        t1.start();
    }
}
