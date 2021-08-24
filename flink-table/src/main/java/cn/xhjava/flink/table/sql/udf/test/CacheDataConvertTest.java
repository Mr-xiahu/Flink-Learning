package cn.xhjava.flink.table.sql.udf.test;

import cn.xhjava.domain.Student3;
import cn.xhjava.flink.table.sql.udf.MyHbaseLookupFuncation;
import com.clinbrain.cachedatetimeconvert.CacheDateConvert;
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
public class CacheDataConvertTest {
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


        CacheDateConvert cacheDataConvert = new CacheDateConvert();

        //注册函数
        tableEnv.registerFunction("cache_date_convert", cacheDataConvert);
        System.out.println("函数注册成功~~~");

        Table table = tableEnv.sqlQuery("select id,name,sex,cache_date_convert('6789') from student");

        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();


    }
}
