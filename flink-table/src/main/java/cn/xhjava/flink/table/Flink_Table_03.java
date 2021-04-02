package cn.xhjava.flink.table;

import cn.xhjava.domain.Student2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/1
 * <p>
 * 将数据输入到表
 */
public class Flink_Table_03 {
    public static void main(String[] args) throws Exception {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        TableEnvironment tableEnvironment = TableEnvironment.create(bbSettings);

        //2.从文本读取真实数据
        DataStreamSource<String> source = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStream<Student2> dataStream = source.map(new MapFunction<String, Student2>() {
            @Override
            public Student2 map(String s) throws Exception {
                String[] split = s.split(",");
                return new Student2(new Integer(split[0]), split[1], split[2]);
            }
        });


        //3.转化为table
        Table student = streamTableEnvironment.fromDataStream(dataStream, "id,name,sex");
        streamTableEnvironment.createTemporaryView("student", student);

        Table result = streamTableEnvironment.sqlQuery("select id,name,sex from student");
        //streamTableEnvironment.toAppendStream(result, Row.class).print();

        /*Schema schema = new Schema()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("sex", DataTypes.STRING());*/

        //输出表
        /*tableEnvironment
                .connect(new FileSystem().path("D:\\git\\study\\Flink-Learning\\"))
                .withFormat(new OldCsv().fieldDelimiter("|").deriveSchema())
                .withSchema(schema)
                .createTemporaryTable("student_tmp");*/
        tableEnvironment.executeSql("create table default_catalog.default_database.student_tmp(id int,name string,sex string)");

        result.executeInsert("student_tmp");
        env.execute();

    }
}
