package cn.xhjava.flink.stream.pojo;

import lombok.Data;

import java.util.List;

/**
 * @author Xiahu
 * @create 2021/5/8
 */
@Data
public class GreenPlumCopyEvent {
    private String table;
    private List<String> dataList;
    private String dataFilePath;
    private String shellPath;
    private String logPath;
}
