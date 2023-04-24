package cn.xhjava.flink.cdc.stream;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */


public class MySqlCdcStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("192.168.0.114")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("flink_realtime")
                .tableList("flink_realtime.t_kafka_offset")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        // enable checkpoint
        env.enableCheckpointing(5000);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1)
                .printToErr().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
