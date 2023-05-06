package cn.xhjava.flink.cdc.stream;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import com.ververica.cdc.debezium.utils.DebeziumOffsetCallback;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */


public class MySqlCdcStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.put("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
        properties.put("database.history.kafka.bootstrap.servers", "192.168.0.113:9092");
        properties.put("database.history.kafka.topic", "dbcenter_init");

        DebeziumOffset specificOffset = new DebeziumOffset();
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("server", "mysql_binlog_source");
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put("transaction_id", null);
        sourceOffset.put("ts_sec", 1683362000);
        sourceOffset.put("file", "mysql-bin.000006");
        sourceOffset.put("pos", 131365950);
        sourceOffset.put("row", 1);
        sourceOffset.put("server_id", 109);
        sourceOffset.put("event", 2);

        specificOffset.setSourcePartition(sourcePartition);
        specificOffset.setSourceOffset(sourceOffset);

        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("192.168.0.114")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("flink_realtime")
                .tableList("flink_realtime.t_data_schema")
//                .startupOptions(StartupOptions.latest())
                .startupOptions(StartupOptions.specificOffset("",0))
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .specificOffset(specificOffset)
                .commitOffset(new DebeziumOffsetCallback() {
                    @Override
                    public void commitOffset(DebeziumOffset debeziumOffset) {
                        System.err.println(debeziumOffset.toString());
                    }
                })
                .build();


        // enable checkpoint
        env.enableCheckpointing(5000);

        env.addSource(sourceFunction)
                .setParallelism(1)
                .printToErr().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
