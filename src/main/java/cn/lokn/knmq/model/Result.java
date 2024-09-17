package cn.lokn.knmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @description: Result for MQServer.
 * @author: lokn
 * @date: 2024/09/11 00:19
 */
@Data
@AllArgsConstructor
public class Result<T> {

    private int code; // 1-success; 0-fail
    private T data;


    public static Result<String> ok() {
        return new Result<>(1, "OK");
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }

    public static Result<KNMessage<?>> msg(String msg) {
        return new Result<>(1, KNMessage.create(msg, null));
    }

    public static Result<KNMessage<?>> msg(KNMessage<?> msg) {
        return new Result<>(1, msg);
    }

    public static Result<List<KNMessage<?>>> msg(List<KNMessage<?>> msg) {
        return new Result<>(1, msg);
    }


}
