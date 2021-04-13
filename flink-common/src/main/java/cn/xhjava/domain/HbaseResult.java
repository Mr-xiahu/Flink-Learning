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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public HashMap<String, String> getColumn() {
        return column;
    }

    public void setColumn(HashMap<String, String> column) {
        this.column = column;
    }
}
