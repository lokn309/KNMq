package cn.lokn.knmq.demo;

import cn.lokn.knmq.client.KNBroker;
import cn.lokn.knmq.client.KNConsumer;
import cn.lokn.knmq.model.KNMessage;
import cn.lokn.knmq.client.KNProducer;
import com.alibaba.fastjson.JSON;

import java.io.IOException;

/**
 * @description:
 * @author: lokn
 * @date: 2024/09/08 23:27
 */
public class KNMqDemo {

    public static void main(String[] args) throws IOException {

        long ids = 0;

        String topic = "cn.lokn.test";
        KNBroker broker = KNBroker.Default;
//        broker.createTopic(topic);

        KNProducer producer = broker.createProducer();
//        KNConsumer<?> consumer = broker.createConsumers(topic);
//        consumer.listen(topic, message -> {
//            System.out.println(" onMessage => " + message);
//        });

        KNConsumer<?> consumer = broker.createConsumers(topic);

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids , "item" + ids, 100 * ids);
            producer.send(topic, new KNMessage<>(ids++, JSON.toJSONString(order), null));
        }

        for (int i = 0; i < 10; i++) {
            KNMessage<?> msg = consumer.recv(topic);
            System.out.println(msg);
            consumer.ack(topic, msg);
        }

        while (true) {
            char c = (char) System.in.read();
            if ('q' == c || c == 'e') {
                break;
            }
            if ('p' == c) {
                Order order = new Order(ids, "item" + ids, ids * 100);
                producer.send(topic, new KNMessage<>(ids++, JSON.toJSONString(order), null));
                System.out.println(" produce ok => " + order);
            }
//            if ('c' == c) {
//                KNMessage<Order> message = (KNMessage<Order>) consumer.recv(topic);
//                System.out.println(" consumer ok => " + message);
//            }
            if ('a' == c) {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids , "item" + ids, 100 * ids);
                    producer.send(topic, new KNMessage<>(ids++, JSON.toJSONString(order), null));
                }
                System.out.println(" produce 10 orders...");
            }
        }

    }

}
