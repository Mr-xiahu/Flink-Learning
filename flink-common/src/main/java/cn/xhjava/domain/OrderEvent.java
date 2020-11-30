package cn.xhjava.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Xiahu
 * @create 2020/11/27
 * 订单表
 */
public class OrderEvent {
    @Getter
    @Setter
    private String orderId;

    @Getter
    @Setter
    private String orderName;

    @Getter
    @Setter
    private Long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String orderName, Long eventTime) {
        this.orderId = orderId;
        this.orderName = orderName;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", orderName='" + orderName + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
