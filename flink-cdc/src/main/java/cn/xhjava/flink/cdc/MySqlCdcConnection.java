package cn.xhjava.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class MySqlCdcConnection {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.0.114")
                .port(3306)
                .databaseList("hid0101_his_cache_xh") // set captured database
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
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();


        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
