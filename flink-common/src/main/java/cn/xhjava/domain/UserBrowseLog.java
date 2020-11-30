package cn.xhjava.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Xiahu
 * @create 2020/11/27
 */
public class UserBrowseLog {
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
    private String pageID;

    public UserBrowseLog() {
    }

    public UserBrowseLog(String userId, String eventTime, String eventType, String pageID) {
        this.userID = userId;
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.pageID = pageID;
    }

    @Override
    public String toString() {
        return "UserBrowseLog{" +
                "userId='" + userID + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", eventType='" + eventType + '\'' +
                ", pageID='" + pageID + '\'' +
                '}';
    }
}
