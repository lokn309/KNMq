package cn.lokn.knmq.core;

import lombok.AllArgsConstructor;

/**
 * @description: producer queue producer.
 * @author: lokn
 * @date: 2024/09/08 23:02
 */
@AllArgsConstructor
public class KNProducer {

    KNBroker broker;

    public boolean send(String topic, KNMessage message) {
        KNMq mq = broker.find(topic);
        if (mq == null) throw new RuntimeException("topic no found");
        return mq.send(message);
    }

}
