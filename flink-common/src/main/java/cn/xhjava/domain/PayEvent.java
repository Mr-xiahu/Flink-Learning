package cn.xhjava.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Xiahu
 * @create 2020/11/27
 * 支付表
 */
public class PayEvent {
    @Getter
    @Setter
    private String payId;

    @Getter
    @Setter
    private String orderId;

    @Getter
    @Setter
    private String pryName;

    @Getter
    @Setter
    private Long pryTime;


    public PayEvent(String payId,String orderId, String pryName, Long pryTime) {
        this.payId = payId;
        this.orderId = orderId;
        this.pryName = pryName;
        this.pryTime = pryTime;
    }

    public PayEvent() {
    }

    @Override
    public String toString() {
        return "PayEvent{" +
                "payId='" + payId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", pryName='" + pryName + '\'' +
                ", pryTime=" + pryTime +
                '}';
    }
}
