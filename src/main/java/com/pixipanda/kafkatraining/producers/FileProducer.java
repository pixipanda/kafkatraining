package com.pixipanda.kafkatraining.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * Created by hduser on 12/5/17.
 */
public class FileProducer {

    //private static Scanner in;
    public static void main(String[] argv)throws Exception {
        if (argv.length != 2) {
            System.err.println("Please specify 2 parameters ");
            System.exit(-1);
        }
        String topicName = argv[0];
        //in = new Scanner(System.in);
        BufferedReader br = new BufferedReader(new FileReader(argv[1]));
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9091,localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer<String, String>(configProperties);
        String line = br.readLine();
        while(line != null) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,line);
            producer.send(rec);//send --> fire and forget
            //producer.send(rec).get();  // get method is blocking call.. Synchronous Producer
            line = br.readLine();
        }
        br.close();
        producer.close();
    }
}
