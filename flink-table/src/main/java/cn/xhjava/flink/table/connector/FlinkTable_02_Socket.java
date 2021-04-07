package cn.xhjava.flink.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/2
 *
 * Table API 向hbase put数据(查询功能暂时没有调通)
 */
public class FlinkTable_02_Socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)\n" +
                "WITH (\n" +
                "  'connector' = 'socket',\n" +
                "  'hostname' = 'node2',\n" +
                "  'port' = '9999',\n" +
                "  'byte-delimiter' = '10',\n" +
                "  'format' = 'changelog-csv',\n" +
                "  'changelog-csv.column-delimiter' = '|'\n" +
                ")");

        Table table = tableEnv.sqlQuery("SELECT name, SUM(score) FROM UserScores GROUP BY name");
        // 查询的结果
        tableEnv.toAppendStream(table, Row.class).print();

        //env.execute();

    }
}
