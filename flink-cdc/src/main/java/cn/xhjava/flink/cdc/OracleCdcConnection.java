package cn.xhjava.flink.cdc;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import deserialize.JsonToOggSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class OracleCdcConnection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DebeziumSourceFunction<String> oracle = OracleSource.<String>builder()
                .hostname("192.168.0.38")
                .port(1521)
                .database("orcl")
                .schemaList("hid0101_his_cache_xh")
                .tableList("hid0101_his_cache_xh.test_1," +
                        "hid0101_his_cache_xh.test_2," +
                        "hid0101_his_cache_xh.test_3," +
                        "hid0101_his_cache_xh.test_4," +
                        "hid0101_his_cache_xh.test_5," +
                        "hid0101_his_cache_xh.test_6," +
                        "hid0101_his_cache_xh.test_7," +
                        "hid0101_his_cache_xh.test_8," +
                        "hid0101_his_cache_xh.test_9," +
                        "hid0101_his_cache_xh.test_10") // set captured table
                .username("klbr")
                .password("klbr")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        // enable checkpoint
        env.enableCheckpointing(3000);

        env.addSource(oracle)
                // set 4 parallel source tasks
                .setParallelism(1)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
