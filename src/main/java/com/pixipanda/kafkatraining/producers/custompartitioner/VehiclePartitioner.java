package com.pixipanda.kafkatraining.producers.custompartitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kafka on 15/1/19.
 */
public class VehiclePartitioner implements Partitioner {

    private static Map<String,Integer> cityToPartitionMap;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //In total there are 6 partitions
        List partitions = cluster.availablePartitionsForTopic(topic);
        int numPartitions = partitions.size();
        String cityName = (String)key;
        if(cityToPartitionMap.containsKey(cityName)){
            //If the city is mapped to particular partition return it
            //Major Metro city is mapped to single partition
            //DELHI ---> vehicle-0
            //MUMBAI ---> vehicle-1
            //KOLKATA ---> vehicle-2
            //CHENNAI ---> vehicle-3
            int majorPartionNo = cityToPartitionMap.get(cityName);
            return majorPartionNo;
        }else {
            //If no city is mapped to particular partition distribute among remaining partitions
            //Messages from minor city will be distributed among vehicle-4 & vehicle-5
            int mapSize = cityToPartitionMap.size();
            int partiionNo =  ((cityName.hashCode() & 0xfffffff) % (numPartitions-mapSize)) + mapSize ;
            return  partiionNo;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

        //This function is called during initialization. HashMap is created mapping City to Parition number.
        //System.out.println("Inside CityPartitioner.configure " + configs);
        cityToPartitionMap = new HashMap<String, Integer>();
        for(Map.Entry<String,?> entry: configs.entrySet()){
            if(entry.getKey().startsWith("partitions.")){
                String keyName = entry.getKey();
                String value = (String)entry.getValue();
                //System.out.println( keyName.substring(11));
                int paritionId = Integer.parseInt(keyName.substring(11));
                cityToPartitionMap.put(value,paritionId);
            }
        }
    }
}
