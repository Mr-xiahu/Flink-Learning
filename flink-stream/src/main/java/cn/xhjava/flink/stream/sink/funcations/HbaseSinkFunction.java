package cn.xhjava.flink.stream.sink.funcations;

import cn.xhjava.domain.Student4;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

/**
 * @author Xiahu
 * @create 2021/4/20
 */
@Slf4j
public class HbaseSinkFunction extends RichSinkFunction<Student4> implements CheckpointedFunction {
    private org.apache.hadoop.conf.Configuration configuration = null;
    private Connection conn = null;
    private Table table = null;
    private String hbaseTableName;
    private ArrayList<Put> puts = new ArrayList<>();

    public HbaseSinkFunction(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node4");
        conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf(hbaseTableName));
    }

    @Override
    public void invoke(Student4 value, Context context) throws Exception {
        Put put = new Put(Bytes.toBytes(value.getId()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("classs"), Bytes.toBytes(value.getClasss()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(value.getCity()));
        puts.add(put);

        if (puts.size() >= 1000) {
            table.put(puts);
            puts.clear();
        }
    }

    @Override
    public void close() throws Exception {
        table.close();
        conn.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("执行Checkpoint,同步快照");
        if (puts.size() > 0) {
            table.put(puts);
            puts.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("初始化状态!");
    }
}
