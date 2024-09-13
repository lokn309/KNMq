package cn.lokn.knmq.client;

import cn.lokn.knmq.model.KNMessage;

/**
 * message listener.
 */
public interface KNListener<T> {

    void onMessage(KNMessage<T> message);

}
