package cn.xhjava.flink.table.Demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/1
 * <p>
 * 使用Api 从kafka中获取数据
 */
public class Flink_Table_06_KafkaPipline {
    public static void main(String[] args) throws Exception {
        //1.构造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2.连接kafka,从kafka内获取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sourcetest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
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

        //输出kafka
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sinktest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                )
                .createTemporaryTable("outputTable");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("outputTable", result);
        statementSet.execute();
        env.execute();




    }
}
