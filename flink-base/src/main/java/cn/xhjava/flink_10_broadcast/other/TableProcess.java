package cn.xhjava.flink_10_broadcast.other;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Xiahu
 * @create 2022/2/16 0016
 */
@Data
public class TableProcess implements Serializable {
    private Integer id;
    private String dbName;
    private String tableName;
    private String mode;
    private String sinkTopic;
    private String sinkParam;
}
