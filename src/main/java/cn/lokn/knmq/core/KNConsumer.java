package cn.lokn.knmq.core;

/**
 * @description: message consumer.
 * @author: lokn
 * @date: 2024/09/08 23:18
 */
public class KNConsumer<T> {

    private KNBroker broker;
    private String topic;
    private KNMq mq;

    public KNConsumer(KNBroker broker) {
        this.broker = broker;
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
