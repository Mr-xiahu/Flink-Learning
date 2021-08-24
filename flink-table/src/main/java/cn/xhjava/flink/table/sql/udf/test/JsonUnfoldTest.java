package cn.xhjava.flink.table.sql.udf.test;

import cn.xhjava.domain.Student3;
import com.clinbrain.cachedatetimeconvert.CacheTimeConvert;
import com.clinbrain.jsonunfold.JsonUnfold;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/8/24 0024
 */
public class JsonUnfoldTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        DataStreamSource<String> sourceStream = env.readTextFile("D:\\code\\XIAHU\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStream<Student3> map = sourceStream.map(new MapFunction<String, Student3>() {
            @Override
            public Student3 map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Student3(String.valueOf(fields[0]), fields[1], fields[2]);
            }
        });

        Table student = tableEnv.fromDataStream(map, "id,name,sex");
        tableEnv.createTemporaryView("student", student);


        JsonUnfold cacheTimeConvert = new JsonUnfold();

        //注册函数
        tableEnv.registerFunction("json_unfold", cacheTimeConvert);
        System.out.println("函数注册成功~~~");

        Table table = tableEnv.sqlQuery("select id,name,sex,json_unfold('{\"name\":\"王小二\",\"age\":25.2,\"birthday\":\"1990-01-01\",\"school\":\"蓝翔\",\"major\":[\"理发\",\"挖掘机\"],\"has_girlfriend\":false,\"car\":null,\"house\":null,\"comment\":\"这是一个注释\"}') from student");

        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();


    }
}
