package cn.xhjava.flink.table.main.hbase;

import cn.xhjava.domain.Student4;
import cn.xhjava.flink.table.sql.udf.MyHbaseLookupFuncationBatch;
import cn.xhjava.flink.table.util.HbaseFamilyParse;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/4/9
 *
 * 改方式行不通
 */
public class FlinkTable_06_UserDefindTableFunction01 {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.0.113:9092");
        prop.setProperty("group.id", "flink_kafka_test");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka_source", new SimpleStringSchema(), prop);
        DataStreamSource<String> source = env.addSource(kafkaSource);
        //DataStreamSource<String> source = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\1000");

        DataStream<Student4> map = source.map(new MapFunction<String, Student4>() {
            @Override
            public Student4 map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Student4(String.valueOf(fields[0]), fields[1], fields[2]);
            }
        });


        Table student = tableEnv.fromDataStream(map, "id,classs,city");
        tableEnv.createTemporaryView("student", student);

        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.0.115");

        // 1.
        HBaseTableSchema schema = HbaseFamilyParse.parseHBaseTableSchema("name,realtime_dim_2_id,sex", "info");
        MyHbaseLookupFuncationBatch baseLookupFunction = new MyHbaseLookupFuncationBatch(configuration, "lookup:realtime_dim_1", schema);

        long start = System.currentTimeMillis();
        //注册函数
        tableEnv.registerFunction("realtime_dim_1", baseLookupFunction);
        System.out.println("函数注册成功~~~");

        Table table = tableEnv.sqlQuery("select rowkey,classs,city,info from student," +
                "LATERAL TABLE(realtime_dim_1(id)) as T(rowkey,info)");

        tableEnv.toAppendStream(table, Row.class).printToErr();

        /*tableEnv.executeSql("CREATE TABLE realtime_result2( \n" +
                "rowkey STRING, \n" +
                "info ROW<name STRING,sex STRING,foreignkey STRING>, \n" +
                "PRIMARY KEY (rowkey) NOT ENFORCED \n" +
                ") WITH ( \n" +
                "'connector' = 'hbase-2.2', \n" +
                "'table-name' = 'lookup:realtime_result2', \n" +
                "'zookeeper.quorum' = '192.168.0.115:2181', \n" +
                "'zookeeper.znode.parent' = '/hbase', \n" +
                "'sink.buffer-flush.interval' = '10m',\n" +
                "'sink.buffer-flush.max-rows' = '1000',\n" +
                "'sink.buffer-flush.interval' = '30s',\n" +
                "'sink.parallelism' = '1'\n" +
                ")");

        TableResult tableResult = tableEnv.executeSql("INSERT INTO realtime_result2 SELECT rowkey, ROW(classs, classs, city) from ( " +
                "select rowkey,classs,city,info from student, " +
                "LATERAL TABLE(realtime_dim_1(id)) as T(rowkey,info)) as info");*/

        //System.out.println(table.explain());
        env.execute();


    }
}
