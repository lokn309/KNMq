package cn.lokn.knmq.server;

import cn.lokn.knmq.model.KNMessage;
import cn.lokn.knmq.store.Indexer;
import cn.lokn.knmq.store.Store;
import com.alibaba.fastjson.JSON;

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
        queues.put("a", new MessageQueue("a"));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();
    private String topic;
//    private KNMessage<?>[] queue = new KNMessage<?>[1024 * 10];
    private Store store = null;
//    private int index;

    public MessageQueue(String topic) {
        this.topic = topic;
        this.store = new Store(topic);
        store.init();
    }

    public static List<KNMessage<?>> batch(String topic, String consumerId, int size) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                nextOffset = offset + entry.getLength();
            }
            KNMessage<?> recv = messageQueue.recv(nextOffset);
            List<KNMessage<?>> result = new ArrayList<>();
            while (recv != null) {
                result.add(recv);
                recv = messageQueue.recv(++offset);
                if (result.size() >= size) {
                    break;
                }
            }
            System.out.println(" ===>> recv: topic/cid/size = " + topic + "/" + consumerId + "/" + result.size());
            System.out.println(" ===>> last message = " + recv);
            return result;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public int send(KNMessage<String> message) {
        int offset = store.pos();
        message.getHeaders().put("X-offset", String.valueOf(offset));
        store.write(message);
        return offset;
    }

    public KNMessage<?> recv(int offset) {
        return store.read(offset);
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
        System.out.println(" ===>> sub: " + subscription);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> unsub: " + subscription);
        if (messageQueue == null) return ;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, KNMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        System.out.println(" ***===>> send: topic/message = " + topic + "/" + message);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    public static KNMessage<?> recv(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(offset);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    // 使用此方法，需要手动调用ack，更新订阅关系里的offset
    public static KNMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int nextOffset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                nextOffset = offset + entry.getLength();
            }
            KNMessage<?> recv = messageQueue.recv(nextOffset);
            System.out.println(" ===>> recv: topic/cid/ind = " + topic + "/" + consumerId + "/" + nextOffset);
            System.out.println(" ===>> message = " + recv);
            return recv;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription messageSubscription = messageQueue.subscriptions.get(consumerId);
            if (offset > messageSubscription.getOffset() && offset < Store.LEN) {
                System.out.println(" ===>> ack: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
                messageSubscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = "
                + topic + "/" + consumerId);
    }

}
