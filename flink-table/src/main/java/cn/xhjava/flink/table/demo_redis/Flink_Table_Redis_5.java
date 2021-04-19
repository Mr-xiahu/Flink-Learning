package cn.xhjava.flink.table.demo_redis;

import cn.xhjava.domain.Student4;
import cn.xhjava.flink.table.udf.tablefuncation.RedisLookupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/4/19
 *
 *  测试redis lookup 4 张维度表 性能
 *
 *  130-140/s
 */
public class Flink_Table_Redis_5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.0.113:9092");
        prop.setProperty("group.id", "flink_kafka");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka_source", new SimpleStringSchema(), prop);
        DataStreamSource<String> source = env.addSource(kafkaSource);


        //DataStreamSource<String> source = env.readTextFile("D:\\code\\github\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStream<Student4> map = source.map(new MapFunction<String, Student4>() {
            @Override
            public Student4 map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Student4(String.valueOf(fields[0]), fields[1], fields[2]);
            }
        });

        Table student = tableEnv.fromDataStream(map, "id,classs,city");
        tableEnv.createTemporaryView("student", student);


        RedisLookupFunction function = new RedisLookupFunction("redis_test_1");
        RedisLookupFunction function2 = new RedisLookupFunction("redis_test_2");
        RedisLookupFunction function3 = new RedisLookupFunction("redis_test_3");
        RedisLookupFunction function4 = new RedisLookupFunction("redis_test_4");
        RedisLookupFunction function5 = new RedisLookupFunction("redis_test_5");


        //注册函数
        tableEnv.registerFunction("redis_test_1", function);
        tableEnv.registerFunction("redis_test_2", function2);
        tableEnv.registerFunction("redis_test_3", function3);
        tableEnv.registerFunction("redis_test_4", function4);
        tableEnv.registerFunction("redis_test_5", function5);

        System.out.println("函数注册成功~~~");

        tableEnv.executeSql("CREATE TABLE realtime_result2( \n" +
                "rowkey STRING, \n" +
                "info ROW<name STRING,sex STRING,foreignkey STRING>, \n" +
                "PRIMARY KEY (rowkey) NOT ENFORCED \n" +
                ") WITH ( \n" +
                "'connector' = 'hbase-2.2', \n" +
                "'table-name' = 'lookup:realtime_result2', \n" +
                "'zookeeper.quorum' = '192.168.0.115:2181', \n" +
                "'zookeeper.znode.parent' = '/hbase', \n" +
                "'sink.buffer-flush.interval' = '100m',\n" +
                "'sink.buffer-flush.max-rows' = '1000',\n" +
                "'sink.buffer-flush.interval' = '300s',\n" +
                "'sink.parallelism' = '1'\n" +
                ")");


        //Table table = tableEnv.sqlQuery("select id,classs,city,line from student, LATERAL TABLE(redis_test_1(id)) as T(line)");
        tableEnv.executeSql("INSERT INTO realtime_result2 SELECT id, ROW(classs, classs, city) from (" +
                "select id,classs,city,line,line2,line3,line4 from student, " +
                "LATERAL TABLE(redis_test_1(id)) as T(line)," +
                "LATERAL TABLE(redis_test_2(id)) as T2(line2)," +
                "LATERAL TABLE(redis_test_3(id)) as T3(line3)," +
                "LATERAL TABLE(redis_test_4(id)) as T4(line4)," +
                "LATERAL TABLE(redis_test_5(id)) as T5(line5)" +
                ") as info");
        //tableEnv.toAppendStream(table, Row.class).printToErr();

        env.execute();

    }
}
