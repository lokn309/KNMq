package cn.lokn.knmq.demo;

import cn.lokn.knmq.core.KNBroker;
import cn.lokn.knmq.core.KNConsumer;
import cn.lokn.knmq.core.KNMessage;
import cn.lokn.knmq.core.KNProducer;

import java.io.IOException;

/**
 * @description:
 * @author: lokn
 * @date: 2024/09/08 23:27
 */
public class KNMqDemo {

    public static void main(String[] args) throws IOException {

        long ids = 0;

        String topic = "kn.order";
        KNBroker broker = new KNBroker();
        broker.createTopic(topic);

        KNProducer producer = broker.createProducer();
        KNConsumer<?> consumer = broker.createConsumers(topic);
        consumer.subscribe(topic);
        consumer.listen(message -> {
            System.out.println(" onMessage => " + message);
        });

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids , "item" + ids, 100 * ids);
            producer.send(topic, new KNMessage<>(ids++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            KNMessage<Order> msg = (KNMessage<Order>) consumer.poll(1000);
            System.out.println(msg);
        }

        while (true) {
            char c = (char) System.in.read();
            if ('q' == c || c == 'e') {
                break;
            }
            if ('p' == c) {
                Order order = new Order(ids, "item" + ids, ids * 100);
                producer.send(topic, new KNMessage<>(ids++, order, null));
                System.out.println(" send ok => " + order);
            }
            if ('c' == c) {
                KNMessage<Order> message = (KNMessage<Order>) consumer.poll(1000);
                System.out.println(" poll ok => " + message);
            }
            if ('a' == c) {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order(ids , "item" + ids, 100 * ids);
                    producer.send(topic, new KNMessage<>(ids++, order, null));
                }
                System.out.println(" send 10 orders...");
            }
        }

    }

}
