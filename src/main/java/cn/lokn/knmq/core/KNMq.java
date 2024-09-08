package cn.lokn.knmq.core;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @description: mq for topic.
 * @author: lokn
 * @date: 2024/09/08 23:07
 */
@AllArgsConstructor
public class KNMq {

    public KNMq(String topic) {
        this.topic = topic;
    }

    private String topic;
    private LinkedBlockingQueue<KNMessage> queue = new LinkedBlockingQueue<>();
    private List<KNListener> listeners = new ArrayList<>();

    public boolean send(KNMessage message) {
        boolean offer = queue.offer(message);
        listeners.forEach(listener -> listener.onMessage(message));
        return offer;
    }

    // 拉模式获取数据
    @SneakyThrows
    public <T> KNMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(KNListener<T> listener) {
        listeners.add(listener);
    }
}
