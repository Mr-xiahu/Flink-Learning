package pojo;

import lombok.Data;

import java.util.Map;

/**
 * @author Xiahu
 * @create 2022/6/16 0016
 */
@Data
public class DebeziumBean {
    private Map<Object,Object> before;
    private Map<Object,Object> after;
    private Source source;
    private String op;
    private String tsMs;
    private String transaction;
}
