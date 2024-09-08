package cn.lokn.knmq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: lokn
 * @date: 2024/09/08 23:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private long id;
    private String item;
    private long price;

}
