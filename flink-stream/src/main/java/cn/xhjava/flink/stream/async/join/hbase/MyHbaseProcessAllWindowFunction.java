package cn.xhjava.flink.stream.async.join.hbase;

import cn.xhjava.flink.stream.pojo.Student4;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/4/21
 */
@Slf4j
public class MyHbaseProcessAllWindowFunction extends ProcessAllWindowFunction<Student4, Student4, TimeWindow> implements CheckpointedFunction {
    private org.apache.hadoop.conf.Configuration configuration = null;
    private Connection conn = null;
    private String hbaseTableName;

    private LinkedHashSet<Table> tableList = new LinkedHashSet<>();

    private Map<String, Map<String, String>> cache = new HashMap<>();


    public MyHbaseProcessAllWindowFunction(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String[] fields = hbaseTableName.split(",");
        for (String table : fields) {
            tableList.add(conn.getTable(TableName.valueOf(table)));
        }
    }

    private List<Get> getList = new ArrayList<>();

    @Override
    public void process(Context context, Iterable<Student4> elements, Collector<Student4> out) throws Exception {
        List<Student4> sourceData = new ArrayList<>();
        List<Student4> tmpJoinData = new ArrayList<>();

        //1.遍历迭代器,获取关联键
        Iterator<Student4> iterator = elements.iterator();
        while (iterator.hasNext()) {
            sourceData.add(iterator.next());
        }

        //join查询
        for (Table table : tableList) {
            String hbaseTableName = table.getName().getNameAsString();
            if (tmpJoinData.isEmpty()) {
                //判断关联建是否存在于缓存中,如果不存在,去查找
                for (int i = 0; i < sourceData.size(); i++) {
                    Student4 student = sourceData.get(i);
                    String key = hbaseTableName + "_" + student.getId();
                    if (!cache.containsKey(key)) {
                        //从hbase中根据key查询数据
                        getList.add(new Get(Bytes.toBytes(student.getId())));
                    } else {
                        Map<String, String> dataMap = cache.get(key);
                        student.setCity(dataMap.get("info:name"));
                        //将关联数据替换后存入临时数据集,等待下次关联
                        tmpJoinData.add(student);
                        //在sourceData中移除该数据
                        sourceData.remove(i);
                    }
                }

                //从Hbase内获取所有的数据,将其存入缓存
                getHbaseData(getList, table);
                getList.clear();

                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student4 student : sourceData) {
                    String key = hbaseTableName + "_" + student.getId();
                    if (cache.containsKey(key)) {
                        Map<String, String> dataMap = cache.get(key);
                        student.setCity(dataMap.get("info:name"));
                        tmpJoinData.add(student);
                    } else {
                        //如果还是没找到,则表示Hbase内不存在关联数据
                        tmpJoinData.add(student);
                    }
                }
                sourceData.clear();
            } else {
                //判断关联建是否存在于缓存中,如果不存在,去查找
                for (int i = 0; i < tmpJoinData.size(); i++) {
                    Student4 student = tmpJoinData.get(i);
                    String key = hbaseTableName + "_" + student.getId();
                    if (!cache.containsKey(key)) {
                        //从hbase中根据key查询数据
                        getList.add(new Get(Bytes.toBytes(student.getId())));
                    } else {
                        Map<String, String> dataMap = cache.get(key);
                        student.setCity(dataMap.get("info:name"));
                        //将关联数据替换后存入临时数据集,等待下次关联
                        sourceData.add(student);
                        //在sourceData中移除该数据
                        tmpJoinData.remove(i);
                    }
                }

                //从Hbase内获取所有的数据,将其存入缓存
                getHbaseData(getList, table);
                getList.clear();

                //再次遍历剩余的SourceData,join上一批次没有关联的数据
                for (Student4 student : tmpJoinData) {
                    String key = hbaseTableName + "_" + student.getId();
                    if (cache.containsKey(key)) {
                        Map<String, String> dataMap = cache.get(key);
                        student.setCity(dataMap.get("info:name"));
                        sourceData.add(student);
                    } else {
                        //如果还是没找到,则表示Hbase内不存在关联数据
                        sourceData.add(student);
                    }
                }
                tmpJoinData.clear();
            }
        }


        if (tmpJoinData.isEmpty()) {
            for (Student4 student : sourceData) {
                out.collect(student);
            }
        } else {
            for (Student4 student : tmpJoinData) {
                out.collect(student);
            }
        }
    }


    public void getHbaseData(List<Get> getList, Table table) throws IOException {
        Result[] results = table.get(getList);
        for (Result result : results) {
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
            map.put("rowkey", rowkey);
            cache.put(table.getName().getNameAsString() + "_" + rowkey, map);
        }
    }


    @Override
    public void close() throws Exception {
        super.close();
        for (Table table : tableList) {
            table.close();
        }
        tableList.clear();
        conn.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("cache size : {}", cache.size());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node4");
        conn = ConnectionFactory.createConnection(configuration);
    }
}
