package cn.xhjava.flink.cdc.stream;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class PostgresCdcStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("192.168.119.128")
                .port(5432)
                .database("postgres")
                .schemaList("public")
                .tableList("public.cdc_pg_source","public.cdc_pg_sink") // set captured table
                .username("postgres")
                .password("postgres")
                .decodingPluginName("wal2json")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.addSource(sourceFunction)
                .printToErr().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
