package cn.lokn.knmq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import cn.lokn.knmq.model.KNMessage;
import cn.lokn.knmq.model.Result;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * @description: broker fo topic.
 * @author: lokn
 * @date: 2024/09/08 23:07
 */
public class KNBroker {

    @Getter
    public static KNBroker Default = new KNBroker();

    public static String brokerUrl = "http://localhost:8765/knmq";

    static {
        init();
    }

    public static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, KNConsumer<?>> consumers = getDefault().getListeners();
            consumers.forEach((topic, consumer1) -> {
                consumer1.forEach(consumer -> {
                    KNMessage<?> recv = consumer.recv(topic);
                    if (recv == null) return;
                    try {
                        consumer.getListener().onMessage(recv);
                        consumer.ack(topic, recv);
                    } catch (Exception e) {
                        // todo
                    }
                });
            });
        }, 100, 100);
    }

    public KNProducer createProducer() {
        return new KNProducer(this);
    }

    public KNConsumer<?> createConsumers(String topic) {
        KNConsumer<?> consumer = new KNConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public boolean send(String topic, KNMessage message) {
        System.out.println(" ===>> send topic/message: " + topic + "/" + message );
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>(){});
        System.out.println(" ===>> send result: " + result);
        return result.getCode() == 1;
    }

    public void sub(String topic, String cid) {
        System.out.println(" ===>> sub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<String>>(){});
        System.out.println(" ===>> sub result: " + result);
    }

    public void unsub(String topic, String cid) {
        System.out.println(" ===>> unsub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + cid,
                new TypeReference<Result<String>>(){});
        System.out.println(" ===>> unsub result: " + result);
    }

    public <T> KNMessage<T> recv(String topic, String cid) {
        System.out.println(" ===>> recv topic/cid: " + topic + "/" + cid);
        Result<KNMessage<String>> result = HttpUtils.httpGet(brokerUrl + "/recv?t=" + topic + "&cid=" + cid, new TypeReference<Result<KNMessage<String>>>() {});
        System.out.println(" ===>> recv result: " + result);
        return (KNMessage<T>) result.getData();
    }

    public boolean ack(String topic, String cid, int offset) {
        System.out.println(" ===>> ack topic/cid/offset: " + topic + "/" + cid + "/" + offset);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/ack?t=" + topic + "&cid=" + cid + "&offset=" + offset,
                new TypeReference<Result<String>>() {});
        System.out.println(" ===>> ack result: " + result);
        return result.getCode() == 1;
    }

    @Getter
    private MultiValueMap<String, KNConsumer<?>> listeners = new LinkedMultiValueMap<>();
    public void addConsumer(String topic, KNConsumer<?> consumer) {
        listeners.add(topic, consumer);
    }
}
