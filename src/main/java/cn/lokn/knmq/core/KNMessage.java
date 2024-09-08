package cn.lokn.knmq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @description: kn message  model.
 * @author: lokn
 * @date: 2024/09/08 22:48
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KNMessage<T> {

    //private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers;    // 系统属性
    //private Map<String, String> properties;   // 业务属性


}
