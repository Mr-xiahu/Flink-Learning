package cn.xhjava.flink.cdc.stream;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.internal.DebeziumOffset;
import com.ververica.cdc.debezium.utils.DebeziumOffsetCallback;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class PostgresCdcStream{
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DebeziumOffset specificOffset = new DebeziumOffset();
        Map<String,String> sourcePartition = new HashMap<>();
        sourcePartition.put("server","postgres_cdc_source");
        Map<String,Object> sourceOffset = new HashMap<>();
        sourceOffset.put("transaction_id",null);
        sourceOffset.put("lsn",(Number)new Long(114118512));
        sourceOffset.put("lsn_proc",(Number)new Long(114118512));
        sourceOffset.put("lsn_commit",(Number)new Long(114118512));
        sourceOffset.put("txId",(Number)new Long(11083));
        sourceOffset.put("ts_usec",new BigInteger("1682412228743943"));

        specificOffset.setSourcePartition(sourcePartition);
        specificOffset.setSourceOffset(sourceOffset);


        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("192.168.119.128")
                .port(5432)
                .database("postgres")
                .schemaList("public")
                .tableList("public.cdc_pg_source","public.cdc_pg_sink") // set captured table
                .username("postgres")
                .password("postgres")
                .decodingPluginName("wal2json")
                .specificOffset(specificOffset)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .commitOffset(new DebeziumOffsetCallback() {
                    @Override
                    public void commitOffset(DebeziumOffset debeziumOffset) {
                        System.err.println(debeziumOffset.toString());
                    }
                })
                .build();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.addSource(sourceFunction)
                .printToErr().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
