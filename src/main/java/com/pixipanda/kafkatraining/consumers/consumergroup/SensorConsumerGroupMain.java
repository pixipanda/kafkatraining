package com.pixipanda.kafkatraining.consumers.consumergroup;

import com.pixipanda.kafkatraining.consumers.SensorProducerThread;

/**
 * Created by kafka on 16/1/19.
 */
public class SensorConsumerGroupMain {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("Please provide command line arguments: groupdId, topic and numberOfConsumer");
            System.exit(-1);
        }

        String brokers = "localhost:9092";
        String groupId = args[0];
        String topic = args[1];
        int numberOfConsumer = Integer.parseInt(args[2]);


        // Start Notification Producer Thread
        SensorProducerThread producerThread = new SensorProducerThread(brokers, topic);
        Thread t1 = new Thread(producerThread);
        t1.start();

        // Start group of Notification Consumers
        SensorConsumerGroup consumerGroup =
                new SensorConsumerGroup(brokers, groupId, topic, numberOfConsumer);

        consumerGroup.execute();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }
}
