package cn.lokn.knmq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description: broker fo topic.
 * @author: lokn
 * @date: 2024/09/08 23:07
 */
public class KNBroker {

    Map<String, KNMq> mqMapping = new ConcurrentHashMap<>(64);

    public KNMq find(String topic) {
        return mqMapping.get(topic);
    }

    public KNMq createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new KNMq(topic));
    }

    public KNProducer createProducer() {
        return new KNProducer(this);
    }

    public KNConsumer<?> createConsumers(String topic) {
        KNConsumer<?> consumer = new KNConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }

}
