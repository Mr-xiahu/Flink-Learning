package cn.xhjava.flink.table.demo;

import cn.xhjava.domain.Student3;
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
 * @create 2021/4/6
 * <p>
 * 流数据实时 look up hbase 双表表数据查询,数据类型 Row
 */
public class FlinkTable_02_HbaseLookupFuncation4 {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        DataStreamSource<String> sourceStream = env.readTextFile("D:\\git\\study\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
//        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.0.113", 8889);
        DataStream<Student3> map = sourceStream.map(new MapFunction<String, Student3>() {
            @Override
            public Student3 map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Student3(String.valueOf(fields[0]), fields[1], fields[2]);
            }
        });

        Table student = tableEnv.fromDataStream(map, "id,name,sex");
        tableEnv.createTemporaryView("student", student);

        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.0.115");
        HBaseTableSchema baseTableSchema = new HBaseTableSchema();
        baseTableSchema.setRowKey("rowkey", String.class);
        baseTableSchema.addColumn("info","small",String.class);
        baseTableSchema.addColumn("info","yellow",String.class);
        baseTableSchema.addColumn("info","man",String.class);
        HBaseLookupFunction baseLookupFunction = new HBaseLookupFunction(configuration, "test:hbase_user_behavior", baseTableSchema);

        HBaseTableSchema baseTableSchema2 = new HBaseTableSchema();
        baseTableSchema2.setRowKey("rowkey", String.class);
        baseTableSchema2.addColumn("info","age",String.class);
        baseTableSchema2.addColumn("info","name",String.class);
        baseTableSchema2.addColumn("info","sex",String.class);
        HBaseLookupFunction baseLookupFunction2 = new HBaseLookupFunction(configuration, "test:hbase_user_behavior2", baseTableSchema2);

        //注册函数
        tableEnv.registerFunction("hbaseLookup", baseLookupFunction);
        tableEnv.registerFunction("hbaseLookup2", baseLookupFunction2);
        System.out.println("函数注册成功~~~");

        Table table = tableEnv.sqlQuery("select id,info1.small,info1.yellow,info1.man,info2 from student,LATERAL TABLE(hbaseLookup(id)) as T(rowkey1,info1),LATERAL TABLE(hbaseLookup2(info1.man)) as T2(rowkey2,info2)");
        //Table table = tableEnv.sqlQuery("select id,name,sex,T from student,LATERAL TABLE(hbaseLookup(id)) as T");

        tableEnv.toAppendStream(table, Row.class).print();
        env.execute();


    }
}
