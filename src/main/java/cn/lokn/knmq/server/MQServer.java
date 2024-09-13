package cn.lokn.knmq.server;

import cn.lokn.knmq.model.KNMessage;
import cn.lokn.knmq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @description: MQ server.
 * @author: lokn
 * @date: 2024/09/10 23:58
 */
@Controller
@RequestMapping("/knmq")
public class MQServer {

    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestParam("cid") String consumerId,
                               @RequestBody KNMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, consumerId, message));
    }

    // recv
    @RequestMapping("/recv")
    public Result<KNMessage<?>> recv(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    // ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    // sub
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic,
                                    @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsub
    @RequestMapping("/unsub")
    public Result<String> unsubscribe(@RequestParam("t") String topic,
                                      @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

}
