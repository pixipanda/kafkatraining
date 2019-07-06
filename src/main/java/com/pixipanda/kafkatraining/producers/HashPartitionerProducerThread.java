package com.pixipanda.kafkatraining.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

/**
 * Created by kafka on 16/1/19.
 */

public class HashPartitionerProducerThread implements Runnable{

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final String[] keys = {"BLR",
        "HYD",
        "PUNE",
        "CHENNAI",
        "MUMBAI",
        "KOLKATA",
        "DELHI",
        "JAIPUR"
        };

    public HashPartitionerProducerThread(String brokers, String topic) {
        Properties prop = createProducerConfig(brokers);
        this.producer = new KafkaProducer<>(prop);
        this.topic = topic;
    }

    private static Properties createProducerConfig(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }



    @Override
    public void run() {
        System.out.println("Produces Started");
        int i = 0;
        Random rnd = new Random();

        try {
        while (true) {
            final String key = keys[rnd.nextInt(this.keys.length)];
            final String value = "sensor" + i;
            ProducerRecord record = new ProducerRecord<String, String>(topic, key, value);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out.println("Sent: { key: " + key + ", value: " + value + " }, Topic: " + topic  + " Partition: " + metadata.partition() + ", Offset: "
                            + metadata.offset());
                }
            });
            i++;
            Thread.sleep(1000);
            }
        }
           catch (InterruptedException ie) {
               producer.close();

            }
        }


    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Please provide command line arguments: topic");
            System.exit(-1);
        }
        String topic = args[0];
        HashPartitionerProducerThread producerThread = new HashPartitionerProducerThread("localhost:9092", topic);
        Thread t1 = new Thread(producerThread);
        t1.start();
    }

}
