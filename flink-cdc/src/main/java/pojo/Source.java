package pojo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Xiahu
 * @create 2022/6/16 0016
 */
@Data
public class Source {
    private String version;
    private String connector;
    private String name;
    private String tsMs;
    private String snapshot;
    private String db;
    private String sequence;
    private String schema;
    private String table;
    private String changeLsn;
    private String commitLsn;
    private String eventSerialNo;
}