package cn.xhjava.flink.table.main.hbase;

import cn.xhjava.domain.Student3;
import cn.xhjava.flink.table.sql.udf.MyHbaseLookupFuncation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;

/**
 * @author Xiahu
 * @create 2021/4/6
 *
 * 流数据实时 look up hbase 单表数据查询
 */
public class FlinkTable_02_HbaseLookupFuncation {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
//        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.0.113", 8889);
        DataStream<Student3> map = sourceStream.map(new MapFunction<String, Student3>() {
            @Override
            public Student3 map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Student3(String.valueOf(fields[0]), fields[1], fields[2]);
            }
        });

        Table student = tableEnv.fromDataStream(map, "id,name,sex");
        tableEnv.createTemporaryView("student", student);


        MyHbaseLookupFuncation baseLookupFunction = new MyHbaseLookupFuncation("test:hbase_user_behavior");

        //注册函数
        tableEnv.registerFunction("hbaseLookup", baseLookupFunction);
        System.out.println("函数注册成功~~~");

        Table table = tableEnv.sqlQuery("select id,name,sex,info from student,LATERAL TABLE(hbaseLookup(id)) as T(info)");

        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();


    }
}
