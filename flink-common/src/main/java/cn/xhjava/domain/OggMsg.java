package cn.xhjava.domain;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/6/12
 */

@Data
public class OggMsg implements Serializable {
    
    private String table;
    
   
    private String op_type;
    
   
    private String op_ts;
    
   
    private String current_ts;
    
   
    private String pos;
    
   
    private List<String> primary_keys;
    
   
    private Map<String, String> before ;
    
   
    private Map<String, String> after;
}
