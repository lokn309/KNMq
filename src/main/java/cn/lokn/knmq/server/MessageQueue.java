package cn.lokn.knmq.server;

import cn.lokn.knmq.model.KNMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: queues.
 * @author: lokn
 * @date: 2024/09/11 00:02
 */
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();
    private static String TEST_TOPIC = "cn.lokn.test";
    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
    private KNMessage<?>[] queue = new KNMessage<?>[1024 * 10];
    private int index;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(KNMessage<?> message) {
        if (index >= queue.length) return -1;
        queue[index++] = message;
        return index;
    }

    public KNMessage<?> recv(int ind) {
        if (ind <= index) {
            return queue[ind];
        }
        return null;
    }

    public void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    private void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) return ;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, String consumerId, KNMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    public static KNMessage<?> recv(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    // 使用此方法，需要手动调用ack，更新订阅关系里的offset
    public static KNMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription messageSubscription = messageQueue.subscriptions.get(consumerId);
            if (offset > messageSubscription.getOffset() && offset <= messageQueue.index) {
                messageSubscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

}
