package cn.xhjava.domain;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author Xiahu
 * @create 2021/4/7
 */
@Data
public class HbaseResult implements Serializable {
    private String tableName;
    private String rowkey;
    private HashMap<String, String> column;


}
