package cn.lokn.knmq.client;

import cn.lokn.knmq.model.KNMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description: message consumer.
 * @author: lokn
 * @date: 2024/09/08 23:18
 */
public class KNConsumer<T> {

    private String id;
    private KNBroker broker;
    private String topic;
    private KNMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public KNConsumer(KNBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        mq = broker.find(topic);
        if (mq == null) throw new RuntimeException("topic not found");
    }

    public KNMessage<T> poll(long timeout) {
        return mq.poll(timeout);
    }

    public void listen(KNListener<T> listener) {
        mq.addListener(listener);
    }

}
