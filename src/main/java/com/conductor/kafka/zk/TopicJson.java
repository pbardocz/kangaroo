package com.conductor.kafka.zk;

import com.google.common.collect.Lists;

import java.util.*;

/**
 * Created by pbardocz on 8/4/15.
 */
public class TopicJson {

    String version;
    HashMap <Integer, ArrayList<Integer>> partitions;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public HashMap<Integer, ArrayList<Integer>> getPartitions() {
        return partitions;
    }

    public void setPartitions(HashMap<Integer, ArrayList<Integer>> partitions) {
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "version='" + version + '\'' +
                ", partitions=" + partitions +
                '}';
    }

    public HashSet<Integer> getTopicBrokersId() {
        HashSet<Integer> topicBrokersId = new HashSet();
        Iterator it = partitions.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            ArrayList<Integer> brokerIds = (ArrayList<Integer>) pair.getValue();
            for(Integer id : brokerIds){
                topicBrokersId.add(id);
            }
        }
        return topicBrokersId;
    }
}
