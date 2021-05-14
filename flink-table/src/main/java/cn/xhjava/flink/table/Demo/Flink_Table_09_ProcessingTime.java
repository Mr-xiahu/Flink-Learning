package cn.xhjava.flink.table.Demo;

import cn.xhjava.domain.Student2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/1
 * <p>
 * 时间特性（Time Attributes） ----- Table 内添加 Processing Time
 */
public class Flink_Table_09_ProcessingTime {
    public static void main(String[] args) throws Exception {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //2.从文本读取真实数据
        DataStreamSource<String> source = env.readTextFile("F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\student");
        DataStream<Student2> dataStream = source.map(new MapFunction<String, Student2>() {
            @Override
            public Student2 map(String s) throws Exception {
                String[] split = s.split(",");
                return new Student2(new Integer(split[0]), split[1], split[2]);
            }
        });

        //3.定义处理时间 Processing Time

        //3.1 由 DataStream 转换成表时指定
        Table dataStr = tableEnv.fromDataStream(dataStream, "id, name,sex,pt.proctime");

        //3.2 定义 Table Schema 时指定,不推荐使用，有点问题
        tableEnv
                .connect(new FileSystem().path("F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\student"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("name", DataTypes.STRING())
                        .field("sex", DataTypes.STRING())
                        //.field("pt", DataTypes.TIMESTAMP(3)).proctime()
                )
                .createTemporaryTable("intputTable");

        Table tableApi = tableEnv.from("intputTable");

        //3.3 创建表的 DDL 中定义
        String sinkDDL =
                "create table dataTable (" +
                        " id varchar(20) not null, " +
                        " name varchar(20), " +
                        " sex varchar(20), " +
                        " pt AS PROCTIME() " +
                        ") with (" +
                        " 'connector.type' = 'filesystem', " +
                        " 'connector.path' = 'F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\student', " +
                        " 'format.type' = 'csv')";
        tableEnv.sqlUpdate(sinkDDL);
        Table sql = tableEnv.sqlQuery("select * from dataTable");


        tableEnv.toAppendStream(dataStr, Row.class).printToErr("dataStr");
        tableEnv.toAppendStream(tableApi, Row.class).printToErr("tableApi");
        tableEnv.toAppendStream(sql, Row.class).printToErr("sql");

        env.execute();

    }
}
