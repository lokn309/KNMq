package cn.lokn.knmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @description: kn message  model.
 * @author: lokn
 * @date: 2024/09/08 22:48
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KNMessage<T> {

    static AtomicLong idgen = new AtomicLong(0);
    //private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>();    // 系统属性
    //private Map<String, String> properties;   // 业务属性

    public static long nextId() {
        return idgen.getAndIncrement();
    }

    public static KNMessage<?> create(String body, Map<String, String > headers) {
        return new KNMessage<>(nextId(), body, headers);
    }

}
