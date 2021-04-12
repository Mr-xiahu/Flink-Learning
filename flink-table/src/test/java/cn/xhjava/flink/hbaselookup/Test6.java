package cn.xhjava.flink.hbaselookup;

import cn.xhjava.domain.Student4;
import cn.xhjava.util.HbaseFamilyParse;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.hbase.source.HBaseLookupFunction;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/4/9
 * kafka 实时数据 lookup hbase维度数据性能测试
 * Hbase 表个数: 5
 */
public class Test6 {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     *  kafka-topics --zookeeper node4:2181/cdh_kafka --delete --topic flink_kafka_source
     *  kafka-topics --zookeeper node4:2181/cdh_kafka --partitions 1 --replication-factor 1 --create --topic flink_kafka_source
     */


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.0.113:9092");
        prop.setProperty("group.id", "flink_kafka_test6");
        //prop.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka_source", new SimpleStringSchema(), prop);
        DataStreamSource<String> source = env.addSource(kafkaSource);
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
        HBaseLookupFunction baseLookupFunction = new HBaseLookupFunction(configuration, "lookup:realtime_dim_1", schema);
        // 2.
        HBaseTableSchema schema2 = HbaseFamilyParse.parseHBaseTableSchema("name,realtime_dim_3_id,sex", "info");
        HBaseLookupFunction baseLookupFunction2 = new HBaseLookupFunction(configuration, "lookup:realtime_dim_2", schema2);
        // 3.
        HBaseTableSchema schema3 = HbaseFamilyParse.parseHBaseTableSchema("name,realtime_dim_4_id,sex", "info");
        HBaseLookupFunction baseLookupFunction3 = new HBaseLookupFunction(configuration, "lookup:realtime_dim_3", schema3);

        // 4.
        HBaseTableSchema schema4 = HbaseFamilyParse.parseHBaseTableSchema("name,realtime_dim_4_id,sex", "info");
        HBaseLookupFunction baseLookupFunction4 = new HBaseLookupFunction(configuration, "lookup:realtime_dim_4", schema4);

        // 4.
        HBaseTableSchema schema5 = HbaseFamilyParse.parseHBaseTableSchema("name,realtime_dim_6_id,sex", "info");
        HBaseLookupFunction baseLookupFunction5 = new HBaseLookupFunction(configuration, "lookup:realtime_dim_5", schema5);
        long start = System.currentTimeMillis();
        //注册函数
        tableEnv.registerFunction("realtime_dim_1", baseLookupFunction);
        tableEnv.registerFunction("realtime_dim_2", baseLookupFunction2);
        tableEnv.registerFunction("realtime_dim_3", baseLookupFunction3);
        tableEnv.registerFunction("realtime_dim_4", baseLookupFunction4);
        tableEnv.registerFunction("realtime_dim_5", baseLookupFunction5);
        System.out.println("函数注册成功~~~");

        Table table = tableEnv.sqlQuery("select id,classs,city,info,info2,info3,info4,info5 from student," +
                "LATERAL TABLE(realtime_dim_1(id)) as T(rowkey,info)," +
                "LATERAL TABLE(realtime_dim_2(info.realtime_dim_2_id)) as T2(rowkey2,info2)," +
                "LATERAL TABLE(realtime_dim_3(info2.realtime_dim_3_id)) as T3(rowkey3,info3)," +
                "LATERAL TABLE(realtime_dim_4(info3.realtime_dim_4_id)) as T4(rowkey4,info4)," +
                "LATERAL TABLE(realtime_dim_5(info4.realtime_dim_4_id)) as T5(rowkey5,info5)");

        //tableEnv.toAppendStream(table, Row.class).print();
        env.execute();
        long end = System.currentTimeMillis();
        long speed = end - start;
        System.out.println("共计耗费时间: " + new Double(speed / 1000.0));

    }
}
