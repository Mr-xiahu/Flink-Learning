package cn.xhjava.flink.table;

import cn.xhjava.domain.Student;
import cn.xhjava.domain.Student2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.hbase.source.HBaseLookupFunction;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Xiahu
 * @create 2021/4/1
 */
public class Demo1 {
    /*public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStream<Student2> mapStream = dataStreamSource.map(new MapFunction<String, Student2>() {
            @Override
            public Student2 map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Student2(Integer.valueOf(fields[0]), fields[1], fields[2]);
            }
        });


        //1.创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.将流转换成表
        Table sensorTable = tableEnv.fromDataStream(mapStream, "id, name ,sex");

        //3.注册自定义函数
        HBaseLookupFunction hBase = new HBaseLookupFunction(
                new Configuration(),
                "hid0101_cache_xdcs_pacs_hj:merge_test16_index",
                new HBaseTableSchema());

        //4.注册hbase lookup 函数
        tableEnv.registerFunction("hbase", hBase);

        //sensorTable

        tableEnv.createTemporaryView("sensor", sensorTable);
        *//* Table table = sensorTable.select("id, name ");*//*
        Table table = tableEnv.sqlQuery("select id,name,hbase(id) from sensor ");

        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();

    }*/

}
