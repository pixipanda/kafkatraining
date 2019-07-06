package com.pixipanda.kafkatraining.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by kafka on 30/1/19.
 */
public class AsynchronousProducerThread implements  Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public AsynchronousProducerThread(String brokers, String topic) {
        Properties prop = createProducerConfig(brokers);
        this.producer = new KafkaProducer<String, String>(prop);
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
        try {
            while (true) {
                final String msg = "msg " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic,msg);
                producer.send(record, new AsynchronousCallback());//Asynchronous Producer
                i++;
                Thread.sleep(1000);
            }
        }
        catch (Exception ie) {
            producer.close();

        }
    }


    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Please provide command line arguments: topic");
            System.exit(-1);
        }
        String topic = args[0];
        AsynchronousProducerThread producerThread = new AsynchronousProducerThread("localhost:9092", topic);
        Thread t1 = new Thread(producerThread);
        t1.start();
    }
}


class AsynchronousCallback implements Callback {

    @Override
    public  void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            System.out.println("AsynchronousProducer failed with an exception");
        else {
            System.out.println("Sent data to partition: " + recordMetadata.partition() + " and offset: " + recordMetadata.offset());
        }
    }
}