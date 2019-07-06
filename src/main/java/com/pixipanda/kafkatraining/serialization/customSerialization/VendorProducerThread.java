package com.pixipanda.kafkatraining.serialization.customSerialization;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;

/**
 * Created by kafka on 8/1/19.
 */
public class VendorProducerThread implements Runnable {

    private final KafkaProducer<String, Vendor> producer;
    private final String topic;

    public VendorProducerThread(String brokers, String topic) {
        Properties prop = createProducerConfig(brokers);
        this.producer = new KafkaProducer<>(prop);
        this.topic = topic;
    }

    private static Properties createProducerConfig(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "VendorSerializer");
        return props;
    }

    @Override
    public void run() {
        System.out.println("Produces Started");
        int i = 0;
        try {
            while (true) {
                int vendorID = i;
                String vendorName = "vendor" + i;
                Date registrationDate = new Date();
                final Vendor vendor = new Vendor(vendorID, vendorName, registrationDate);
                ProducerRecord<String, Vendor> record = new ProducerRecord<>(topic, vendor);
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                        System.out.println("Sent:" + vendor + ", Topic: " + topic  + " Partition: " + metadata.partition() + ", Offset: "
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
        VendorProducerThread producerThread = new VendorProducerThread("localhost:9092", topic);
        Thread t1 = new Thread(producerThread);
        t1.start();
    }
}
