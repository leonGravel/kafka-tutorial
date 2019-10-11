package com.gravel.kafkaTutorial.config;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;
import org.apache.kafka.common.utils.Utils;

import java.util.*;

/**
 * @author Gravel
 * @date 2019/10/11.
 */
public class AverageTopicAssignor extends AbstractPartitionAssignor {

    @Override
    public String name() {
        return "averagetopic";
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, List<String>> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>(subscriptions.keySet().size() * 2);
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }

        CircularIterator<String> assigner = new CircularIterator<>(Utils.sorted(subscriptions.keySet()));
        Map<String, List<TopicPartition>> map = allPartitionsMap(partitionsPerTopic, subscriptions);

        for (List<TopicPartition> topicPartitions : map.values()) {
            assignment.get(assigner.next()).addAll(topicPartitions);
        }
        return assignment;
    }

    private Map<String, List<TopicPartition>> allPartitionsMap(Map<String, Integer> partitionsPerTopic, Map<String, List<String>> subscriptions) {
        SortedSet<String> topics = new TreeSet<>();
        for (List<String> subscription : subscriptions.values()) {
            topics.addAll(subscription);
        }
        Map<String, List<TopicPartition>> partitionsMap = new HashMap<>(topics.size() * 2);
        for (String topic : topics) {
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic != null) {
                partitionsMap.put(topic, AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic));
            }
        }
        return partitionsMap;
    }

}
