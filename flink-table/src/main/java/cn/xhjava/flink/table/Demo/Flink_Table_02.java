package cn.xhjava.flink.table.Demo;

import cn.xhjava.domain.Student2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/1
 * <p>
 * 使用新API,Flink-sql类型,创建表并打印数据
 */
public class Flink_Table_02 {
    public static void main(String[] args) throws Exception {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2.从文本读取真实数据
        DataStreamSource<String> source = env.readTextFile("F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStream<Student2> dataStream = source.map(new MapFunction<String, Student2>() {
            @Override
            public Student2 map(String s) throws Exception {
                String[] split = s.split(",");
                return new Student2(new Integer(split[0]), split[1], split[2]);
            }
        });


        //3.转化为table
        Table student = tableEnv.fromDataStream(dataStream, "id,name,sex");
        tableEnv.createTemporaryView("student", student);
        tableEnv.sqlQuery("select id,name,sex from student");
        tableEnv.toAppendStream(student, Row.class).print();

        env.execute();

    }
}
