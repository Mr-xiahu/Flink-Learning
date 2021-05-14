package cn.xhjava.flink.table.main.hbase;

import cn.xhjava.domain.Student2;
import cn.xhjava.flink.table.sql.udf.MyHbaseLookupFuncation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/6
 * <p>
 * 流数据实时 look up hbase 多表数据查询
 */
public class FlinkTable_03_HbaseLookupFuncation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
//        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.0.113", 8888);
        DataStream<Student2> map = sourceStream.map(new MapFunction<String, Student2>() {
            @Override
            public Student2 map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Student2(Integer.valueOf(fields[0]), fields[1], fields[2]);
            }
        });

        Table student = tableEnv.fromDataStream(map, "id,name,sex");
        tableEnv.createTemporaryView("student", student);

        // 注册第一张Hbase维度表
        MyHbaseLookupFuncation baseLookupFunction = new MyHbaseLookupFuncation("test:hbase_user_behavior");
        // 注册第二张Hbase维度表
        MyHbaseLookupFuncation baseLookupFunction2 = new MyHbaseLookupFuncation("test:hbase_user_behavior2");

        //注册函数
        tableEnv.registerFunction("hbaseLookup", baseLookupFunction);
        tableEnv.registerFunction("hbaseLookup2", baseLookupFunction2);
        System.out.println("函数注册成功~~~");

        //查询两张表
        Table table = tableEnv.sqlQuery("select id,info,info2 from student,LATERAL TABLE(hbaseLookup(id)) as T(info),LATERAL TABLE(hbaseLookup2(id)) as T2(info2)");


        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();

        /**
         *  如果hbase table1内存在rowkey = 2000 ,hbase table2 也存在rowkey = 2000 ,那么该条数据可以成功查询
         *  如果hbase table1内存在rowkey = 2000 ,但是hbase table2 不存在rowkey = 2000 ,那么该条数据不能成功查询
         *
         *  可以在eval() 内手动设置
         */
    }
}
