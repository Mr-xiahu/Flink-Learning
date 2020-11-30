package cn.xhjava.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Xiahu
 * @create 2020/11/27
 */
public class UserClickLog {
    @Getter
    @Setter
    private String userID;

    @Getter
    @Setter
    private String eventTime;

    @Getter
    @Setter
    private String eventType;

    @Getter
    @Setter
    private String productID;

    @Getter
    @Setter
    private Integer productPrice;

    public UserClickLog() {
    }

    public UserClickLog(String userId, String eventTime, String eventType, String productID, Integer productPrice) {
        this.userID = userId;
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.productID = productID;
        this.productPrice = productPrice;
    }

    @Override
    public String toString() {
        return "UserClickLog{" +
                "userId='" + userID + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", eventType='" + eventType + '\'' +
                ", productID='" + productID + '\'' +
                ", productPrice=" + productPrice +
                '}';
    }
}
