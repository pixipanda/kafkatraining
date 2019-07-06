package com.pixipanda.kafkatraining.serialization;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by kafka on 8/1/19.
 */
public class StringSerializationThread implements Runnable {

    private final KafkaProducer<Long, String> producer;
    private final String topic;

    public StringSerializationThread(String brokers, String topic) {
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
        props.put("key.serializer",LongSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return props;
    }

    @Override
    public void run() {
        System.out.println("Produces Started");
        int i = 0;
        try {
            while (true) {
                Long key = new Long(i);
                final String value = "msg " + i;
                ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                        System.out.println("Sent:" + value + ", Topic: " + topic  + " Partition: " + metadata.partition() + ", Offset: "
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
        StringSerializationThread producerThread = new StringSerializationThread("localhost:9092", topic);
        Thread t1 = new Thread(producerThread);
        t1.start();
    }
}
