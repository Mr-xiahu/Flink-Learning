/*
package cn.xhjava.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

*/
/**
 * @author Xiahu
 * @create 2021/10/27 0027
 *//*

public class MySqlCdcConnection {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        */
/*MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.0.114")
                .port(3306)
                .databaseList("flink_realtime") // set captured database
                .tableList("flink_realtime.t_table_process") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();*//*


        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("192.168.0.114")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("flink_realtime")
                .tableList("flink_realtime.t_table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();


        // enable checkpoint
        env.enableCheckpointing(60000);

        env.addSource(build)
                .setParallelism(1)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
*/
