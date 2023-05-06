package cn.xhjava.flink.cdc.stream;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class OracleCdcStreamLogMiner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
//        properties.put("database.connection.adapter", "xstream");
//        properties.put("database.out.server.name", "dbzxout");
        properties.put("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
        properties.put("database.history.kafka.bootstrap.servers", "192.168.0.113:9092");
        properties.put("database.history.kafka.topic", "dbcenter_init");


        DebeziumSourceFunction<String> oracle = OracleSource.<String>builder()
                .hostname("192.168.0.67")
                .port(1521)
                .database("dbcenter")
                .schemaList("HID0101_CACHE_HIS_CDCTEST_XH")
                .tableList("HID0101_CACHE_HIS_CDCTEST_XH.TEST_1")
                .username("klbr")
                .password("klbr")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .build();


        // enable checkpoint
        env.enableCheckpointing(3000);

        env.addSource(oracle)
                // set 4 parallel source tasks
                .setParallelism(1)
                .printToErr().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}