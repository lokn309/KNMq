package cn.lokn.knmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description: Message subscription.
 * @author: lokn
 * @date: 2024/09/11 00:12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;

}
