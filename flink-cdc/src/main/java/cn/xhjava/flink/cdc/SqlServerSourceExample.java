package cn.xhjava.flink.cdc;

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
public class SqlServerSourceExample {
    public static void main(String[] args) throws Exception {
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        env.setParallelism(1);
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //env.setStateBackend(new RocksDBStateBackend("file:///usr/local/flink-1.13.5/ck"));

        env.addSource(sourceFunction).print();

        env.execute("1");

    }
}
