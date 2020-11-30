package cn.xhjava.domain;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/6/12
 */
public class OggMsg implements Serializable {
    @Getter
    @Setter
    private String table;
    @Getter
    @Setter
    private String op_type;
    @Getter
    @Setter
    private String op_ts;
    @Getter
    @Setter
    private String current_ts;
    @Getter
    @Setter
    private String pos;
    @Getter
    @Setter
    private List<String> primary_keys;
    @Getter
    @Setter
    private Map<String, String> before = new HashMap<>();
    @Getter
    @Setter
    private Map<String, String> after;

//    public String getTable() {
//        return table;
//    }
//
//    public void setTable(String table) {
//        this.table = table;
//    }
//
//    public String getOp_type() {
//        return op_type;
//    }
//
//    public void setOp_type(String op_type) {
//        this.op_type = op_type;
//    }
//
//    public String getOp_ts() {
//        return op_ts;
//    }
//
//    public void setOp_ts(String op_ts) {
//        this.op_ts = op_ts;
//    }
//
//    public String getCurrent_ts() {
//        return current_ts;
//    }
//
//    public void setCurrent_ts(String current_ts) {
//        this.current_ts = current_ts;
//    }
//
//    public String getPos() {
//        return pos;
//    }
//
//    public void setPos(String pos) {
//        this.pos = pos;
//    }
//
//    public List<String> getPrimary_keys() {
//        return primary_keys;
//    }
//
//    public void setPrimary_keys(List<String> primary_keys) {
//        this.primary_keys = primary_keys;
//    }
//
//    public Map<String, String> getBefore() {
//        return before;
//    }
//
//    public void setBefore(Map<String, String> before) {
//        this.before = before;
//    }
//
//    public Map<String, String> getAfter() {
//        return after;
//    }
//
//    public void setAfter(Map<String, String> after) {
//        this.after = after;
//    }


    @Override
    public String toString() {
        return "OggMsg{" +
                "table='" + table + '\'' +
                ", op_type='" + op_type + '\'' +
                ", op_ts='" + op_ts + '\'' +
                ", current_ts='" + current_ts + '\'' +
                ", pos='" + pos + '\'' +
                ", primary_keys=" + primary_keys +
                ", before=" + before +
                ", after=" + after +
                '}';
    }
}
