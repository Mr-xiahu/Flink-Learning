package cn.xhjava.flink.hbaselookup;

import cn.xhjava.domain.Student3;
import cn.xhjava.domain.Student4;
import cn.xhjava.util.HbaseFamilyParse;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.hbase.source.HBaseLookupFunction;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import java.text.SimpleDateFormat;

/**
 * @author Xiahu
 * @create 2021/4/9
 * socket 实时数据 lookup hbase维度数据性能测试
 * Hbase 表个数: 1
 */
public class Test1 {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\1000");
//        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\10000");
//        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\100000");
//        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\1000000");
        DataStream<Student4> map = sourceStream.map(new MapFunction<String, Student4>() {
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
        HBaseTableSchema schema = HbaseFamilyParse.parseHBaseTableSchema("name,realtime_dim_2_id,sex", "info");
        HBaseLookupFunction baseLookupFunction = new HBaseLookupFunction(configuration, "lookup:realtime_dim_1", schema);
        long start = System.currentTimeMillis();
        //注册函数
        tableEnv.registerFunction("realtime_dim_1", baseLookupFunction);
        System.out.println("函数注册成功~~~");


        Table table = tableEnv.sqlQuery("select id,classs,city,info from student," +
                "LATERAL TABLE(realtime_dim_1(id)) as T(rowkey,info)");

        tableEnv.toAppendStream(table, Row.class).printToErr();


        env.execute();

    }
}
