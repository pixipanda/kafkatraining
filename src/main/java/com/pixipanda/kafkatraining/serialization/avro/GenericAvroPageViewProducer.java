package com.pixipanda.kafkatraining.serialization.avro;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by kafka on 29/1/19.
 */

public class GenericAvroPageViewProducer implements Runnable {
    private final KafkaProducer<String, GenericRecord> producer;
    private final String topic;
    private final Schema schema;

    /*private final String[] referrer = {"http://www.goole.com/",
            "http://www.facebook/",
            "http://www.twitter.com/",
            "http://www.timesofindia/",
            "http://www.washingtonpost.com/",
            "http://www.firstpost.com/",
            "http://www.nytimes.com/",
            "http://www.thehindu.com/",
            "http://www.hindustantimes.com/",
            "http://www.livemint.com/"};*/

    public GenericAvroPageViewProducer(String brokers, String topic, String schemaUrl) {
        Properties prop = createProducerConfig(brokers, schemaUrl);
        this.producer = new KafkaProducer<String, GenericRecord>(prop);
        this.topic = topic;
        this.schema = getSchema(schemaUrl, topic);
    }

    private static Properties createProducerConfig(String brokers, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas",false);

        return props;
    }


    private  Schema getSchema() {
        Schema schema = null;
        try {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(this.getClass().getResourceAsStream("/pageview-value.avsc"));
        }
        catch (IOException e) {
            System.out.println("Schema file not found");
        }
        return schema;
    }


    private  Schema getSchema(String SchemaRegistryUrl, String topic) {
        Schema schema = null;
        String subject = topic + "-value";
        RestService restService = new RestService(SchemaRegistryUrl);
        try {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(restService.getLatestVersion(subject).getSchema());
        }
        catch (RestClientException | IOException e) {
         System.out.println("RestClient Exception" + e);
        }
        return  schema;
    }

    @Override
    public void run() {
        System.out.println("Produces Started");
        int i = 0;
        Random rnd = new Random();

        try {

            while (true) {

                final GenericRecord pageView = new GenericData.Record(schema);
                pageView.put("page", "pixipanda/page" + (rnd.nextInt(100) + 1));
                String ip = "192.168.2." + rnd.nextInt(255);
                pageView.put("ip", ip);
                long runtime = new Date().getTime();
                pageView.put("time", runtime);
                /*String referrer = this.referrer[rnd.nextInt(this.referrer.length)];
                pageView.put("referrer", referrer);*/

                producer.send(new ProducerRecord<String, GenericRecord>(topic, pageView), new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                        System.out.println("Sent:" + pageView + ", Topic: " + topic + " Partition: " + metadata.partition() + ", Offset: "
                                + metadata.offset());
                    }
                });
                i++;
                Thread.sleep(1000);
            }
        }
        catch (Exception ie) {
            System.out.println("Failed to send message to kafka" + ie);
            producer.close();

        }
    }


    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Please provide command line arguments: topic & SchemaRegistryUrl");
            System.exit(-1);
        }
        String topic = args[0];
        String schemaRegistryUrl = args[1];
        GenericAvroPageViewProducer producerThread = new GenericAvroPageViewProducer("localhost:9092", topic,schemaRegistryUrl);
        Thread t1 = new Thread(producerThread);
        t1.start();
    }
}
