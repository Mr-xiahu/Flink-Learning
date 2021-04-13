package cn.xhjava.domain;

import lombok.Data;

/**
 * @author Xiahu
 * @create 2021/4/9
 */
@Data
public class HbaseUserBehavior {
    private String rowkey;
    private String man;
    private String small;
    private String yellow;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getMan() {
        return man;
    }

    public void setMan(String man) {
        this.man = man;
    }

    public String getSmall() {
        return small;
    }

    public void setSmall(String small) {
        this.small = small;
    }

    public String getYellow() {
        return yellow;
    }

    public void setYellow(String yellow) {
        this.yellow = yellow;
    }
}
