package cn.lokn.knmq.client;

import cn.lokn.knmq.model.KNMessage;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description: message consumer.
 * @author: lokn
 * @date: 2024/09/08 23:18
 */
public class KNConsumer<T> {

    private String id;
    private KNBroker broker;
    private KNMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public KNConsumer(KNBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void sub(String topic) {
        broker.sub(topic, id);
    }

    public void unsub(String topic) {
        broker.unsub(topic, id);
    }

    public KNMessage<T> recv(String topic) {
        return broker.recv(topic, id);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public boolean ack(String topic, KNMessage<?> message) {
        int offset = Integer.valueOf(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public void listen(String topic, KNListener<T> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

    @Getter
    private KNListener listener;

}
