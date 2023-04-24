package cn.xhjava.flink.cdc.stream;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import deserialize.JsonToOggSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Xiahu
 * @create 2022/6/16 0016
 */
public class SqlServerCdcStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<String> sourceFunction = SqlServerSource.<String>builder()
                .hostname("192.168.0.36")
                .port(1433)
                .database("hid0101_cache_his_xh") // monitor sqlserver database
                .tableList("dbo.test_2","dbo.test_3")
                .username("sa")
                .password("P@ssw0rd")
                .deserializer(new JsonToOggSchema())
                .startupOptions(StartupOptions.initial())
                .build();



        // enable checkpoint
        env.enableCheckpointing(3000);

        env.addSource(sourceFunction)
                .printToErr().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();

    }
}
