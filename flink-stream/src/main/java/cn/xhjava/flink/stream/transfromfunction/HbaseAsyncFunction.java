package cn.xhjava.flink.stream.transfromfunction;

import cn.xhjava.flink.stream.pojo.Student4;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2021/4/20
 *
 * 实时流 异步关联hbase
 */
@Slf4j
public class HbaseAsyncFunction extends RichAsyncFunction<Student4, Student4> implements CheckpointedFunction {
    private org.apache.hadoop.conf.Configuration configuration = null;
    private Connection conn = null;
    private Table table = null;
    private String hbaseTableName;

    private Cache<String, Map<String, String>> cache;

    public HbaseAsyncFunction(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        cache = CacheBuilder
                .newBuilder()
                .maximumSize(2000000)
                //.expireAfterAccess(10, TimeUnit.MINUTES)
                .build();
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node4");
        conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf(hbaseTableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            CellScanner cellScanner = result.cellScanner();
            HashMap<String, String> map = new HashMap<>();
            String rowkey = null;
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                byte[] rowArray = cell.getRowArray();//rowkey
                byte[] familyArray = cell.getFamilyArray();//family
                byte[] qualifierArray = cell.getQualifierArray();//列
                byte[] values = cell.getValueArray();
                rowkey = new String(rowArray, cell.getRowOffset(), cell.getRowLength());
                String info = new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength());
                String column = new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength());
                String value = new String(values, cell.getValueOffset(), cell.getValueLength());
                map.put(info + ":" + column, value);
            }
            cache.put(rowkey, map);
        }

    }

    @Override
    public void asyncInvoke(Student4 input, ResultFuture<Student4> resultFuture) throws Exception {
        Map<String, String> cacheData = cache.getIfPresent(input.getId());
        if (null == cacheData) {
            getFromHbase(input);
        }
        cacheData = cache.getIfPresent(input.getId());
        if (null != cacheData) {
            input.setClasss(cacheData.get("info:name"));
            resultFuture.complete(Collections.singleton(input));
        } else {
            resultFuture.complete(Collections.singleton(input));
        }
    }

    @Override
    public void timeout(Student4 input, ResultFuture<Student4> resultFuture) throws Exception {
//        getFromHbase(input);

    }

    public String forEachMap(Student4 input, Map<String, String> cacheData) {
        StringBuffer sb = new StringBuffer();
        sb.append(input.getId()).append(",");
        sb.append(input.getClass()).append(",");
        sb.append(input.getCity()).append(",");
        Iterator<Map.Entry<String, String>> iterator = cacheData.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            sb.append(entry.getKey()).append(",").append(entry.getValue()).append(",");
        }
        return sb.toString();
    }


    public void getFromHbase(Student4 input) throws IOException {
        Get get = new Get(Bytes.toBytes(input.getId()));
        Result result = null;
        String rowkey = null;
        try {
            result = table.get(get);
            if (!result.isEmpty()) {
                rowkey = Bytes.toString(result.getRow());
                CellScanner cellScanner = result.cellScanner();
                HashMap<String, String> map = new HashMap<>();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    byte[] rowArray = cell.getRowArray();//rowkey
                    byte[] familyArray = cell.getFamilyArray();//family
                    byte[] qualifierArray = cell.getQualifierArray();//列
                    byte[] values = cell.getValueArray();
                    rowkey = new String(rowArray, cell.getRowOffset(), cell.getRowLength());
                    String info = new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength());
                    String column = new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = new String(values, cell.getValueOffset(), cell.getValueLength());
                    map.put(info + ":" + column, value);
                }
                cache.put(rowkey, map);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(input.toString());
            log.error(rowkey);
        }

    }

    @Override
    public void close() throws Exception {
        table.close();
        conn.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("cache size: {}", cache.size());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
