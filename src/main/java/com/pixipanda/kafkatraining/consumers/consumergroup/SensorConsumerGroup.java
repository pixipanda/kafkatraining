package com.pixipanda.kafkatraining.consumers.consumergroup;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kafka on 16/1/19.
 */
public class SensorConsumerGroup {

    private final int numberOfConsumers;
    private final String groupId;
    private final String topic;
    private final String brokers;
    private List<SensorConsumerThread> consumers;

    public SensorConsumerGroup(String brokers, String groupId, String topic,
                                     int numberOfConsumers) {
        this.brokers = brokers;
        this.topic = topic;
        this.groupId = groupId;
        this.numberOfConsumers = numberOfConsumers;
        consumers = new ArrayList<>();
        for (int i = 0; i < this.numberOfConsumers; i++) {
            SensorConsumerThread ncThread =
                    new SensorConsumerThread(this.brokers, this.groupId, this.topic);
            consumers.add(ncThread);
        }
    }

    public void execute() {
        for (SensorConsumerThread ncThread : consumers) {
            Thread t = new Thread(ncThread);
            t.start();
        }
    }

    /**
     * @return the numberOfConsumers
     */
    public int getNumberOfConsumers() {
        return numberOfConsumers;
    }

    /**
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }
}
