package cn.xhjava.flink.table.Demo;

import cn.xhjava.domain.Student2;
import cn.xhjava.domain.Student3;
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
 * Table Api 转换查询操作
 */
public class Flink_Table_04 {
    public static void main(String[] args) throws Exception {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String filePath = "F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table student = tableEnv.from("inputTable");

        //3.查询转换

        //3.1 Table Api

        //简单查询
        Table result = student.select("id,timestamp").filter("id === 'sensor_1'");

        //聚合查询
        Table agg = student.groupBy("id").select("id, id.count as count, temp.avg as avgTemp");

        //3.2 SQL 查询
        Table sqlResult = tableEnv.sqlQuery("select id, temp from inputTable where id = 'senosr_6'");
        Table sqlAgg = tableEnv.sqlQuery("select id,count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");


        tableEnv.toAppendStream(result, Row.class).print("result");
        //在使用toAppendStream() 打印聚合操作后的表时会报错
        //tableEnv.toAppendStream(agg, Row.class).print("agg");
        //因为toAppendStream 只是追加,但是聚合操作会修改表之前的数据
        //所以需要用toRetractStream()打印输出
        tableEnv.toRetractStream(agg, Row.class).print("agg");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");
        tableEnv.toRetractStream(sqlAgg, Row.class).print("sqlAgg");
        env.execute();

        /**
         * 思考?
         * 如果数据现在需要被修改,比如:聚合的数据需要修改,并落地到第三方存储系统,如何操作呢?
         * 对于流式查询,需要声明如何在表和外部连接器之间的执行转换
         * 与外部系统交换的消息类型,由更新模式(Update Mode) 指定
         * 1.追加模式(Append):
         *      表只做插入操作,与外部连接器只交换插入(Insert)信息
         * 2.撤回模式(Retract):
         *      表和外部连接器交换添加(Add) 和 撤回(Retract)消息
         *      插入(Insert)操作编码为Add消息
         *      删除(Delete)操作编码为Rectract消息
         *      更新(Update)编码为上一条的Rectract消息和下一条的Add消息
         * 3.更新插入模式(Upsert):
         *      更新和插入操作都被编码为Update消息
         *      删除操作被编码为Delete消息
         */

    }
}
