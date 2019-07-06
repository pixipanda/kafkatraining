package com.pixipanda.kafkatraining.consumers.rebalance;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by kafka on 21/1/19.
 */
public class RebalanceHandlerExample implements  Runnable{

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private Map<TopicPartition, OffsetAndMetadata> processedOffsets = new HashMap<>();

    private class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition>
                                                 partitions) {
            System.out.println("New Partition Assigned");
        }

        public void onPartitionsRevoked(Collection<TopicPartition>
                                                partitions) {
            System.out.println("Lost partitions in rebalance.  Committing processed offsets:" + processedOffsets);
            consumer.commitSync(processedOffsets);
        }
    }

    public RebalanceHandlerExample(String brokers, String groupId, String topic) {
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic), new HandleRebalance());
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
                    processedOffsets.put(new TopicPartition(record.topic(),
                            record.partition()), new
                            OffsetAndMetadata(record.offset() + 1, "no metadata"));
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
        RebalanceHandlerExample consumer = new RebalanceHandlerExample("localhost:9092", groupId, topic);
        Thread t1 = new Thread(consumer);
        t1.start();
    }
}
