package cn.xhjava.flink.table.Demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.StatementSet;
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
 *
 */
public class Flink_Table_04_UpdateModel {
    public static void main(String[] args) throws Exception {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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


        //简单查询
        Table result = student.select("id,timestamp").filter("id === 'sensor_1'");

        tableEnv.toAppendStream(result, Row.class).print("result");

        //输出到外部文件
        String outFile = "F:\\git\\Flink-Learning\\flink-table\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outFile))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                )
                //todo 设置更新模式
//                .inAppendMode()
//                .inRetractMode()
//                .inUpsertMode()
                .createTemporaryTable("outputTable");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("outputTable", result);
        statementSet.execute();
        env.execute();

    }
}
