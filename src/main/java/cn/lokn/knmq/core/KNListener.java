package cn.lokn.knmq.core;

/**
 * message listener.
 */
public interface KNListener<T> {

    void onMessage(KNMessage<T> message);

}
