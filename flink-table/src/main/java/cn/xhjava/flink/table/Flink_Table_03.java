package cn.xhjava.flink.table;

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
 * 使用过期API,创建表并做其他操作
 */
public class Flink_Table_03 {
    public static void main(String[] args) throws Exception {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //输出表
        tableEnv
                .connect(new FileSystem().path("F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\student"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("sex", DataTypes.STRING()))
                .createTemporaryTable("intputTable");

        Table student = tableEnv.from("intputTable");
        tableEnv.toAppendStream(student, Row.class).printToErr();
        env.execute();

    }
}
