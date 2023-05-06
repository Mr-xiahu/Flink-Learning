package cn.xhjava.flink.cdc.stream;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import com.ververica.cdc.debezium.utils.DebeziumOffsetCallback;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class OracleCdcStreamXstream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DebeziumOffset specificOffset = new DebeziumOffset();
        Map<String,String> sourcePartition = new HashMap<>();
        sourcePartition.put("server","oracle_logminer");
        Map<String,Object> sourceOffset = new HashMap<>();
        sourceOffset.put("transaction_id",null);
        sourceOffset.put("lcr_position",new String("0000021f984e00000001000000010000021f984d000000010000000101"));
        /*sourceOffset.put("scn",new String("35623143"));
        sourceOffset.put("snapshot",new String("35623143"));
        sourceOffset.put("snapshot_completed",new String("35623143"));*/
        specificOffset.setSourcePartition(sourcePartition);
        specificOffset.setSourceOffset(sourceOffset);


        Properties properties = new Properties();
        properties.put("database.connection.adapter", "xstream");
        properties.put("database.out.server.name", "dbzxout");
        properties.put("database.history", "io.debezium.relational.history.KafkaDatabaseHistory");
        properties.put("database.history.kafka.bootstrap.servers", "192.168.0.113:9092");
        properties.put("database.history.kafka.topic", "dbcenter_init");


        DebeziumSourceFunction<String> oracle = OracleSource.<String>builder()
                .hostname("192.168.0.67")
                .port(1521)
                .database("dbcenter")
                .schemaList("KLBR")
                .tableList("KLBR.PRODUCT") // set captured table
                .username("xstrm")
                .password("xstrm")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .specificOffset(specificOffset)
                .commitOffset(new DebeziumOffsetCallback() {
                    @Override
                    public void commitOffset(DebeziumOffset debeziumOffset) {
                        System.err.println(debeziumOffset.toString());
                    }
                })
                .build();


        // enable checkpoint
        env.enableCheckpointing(3000);

        env.addSource(oracle)
                // set 4 parallel source tasks
                .setParallelism(1)
                .printToErr().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print Oracle Snapshot + Binlog");
    }
}
