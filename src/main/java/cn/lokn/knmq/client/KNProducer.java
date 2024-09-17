package cn.lokn.knmq.client;

import cn.lokn.knmq.model.KNMessage;
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
        return broker.send(topic, message);
    }

}
