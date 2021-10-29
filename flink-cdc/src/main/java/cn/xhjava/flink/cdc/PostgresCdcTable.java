package cn.xhjava.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class PostgresCdcTable {
    public static void main(String[] args) {
        //设置flink表环境变量
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        //获取flink流环境变量
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        exeEnv.setParallelism(1);

        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv, fsSettings);

        //拼接souceDLL
        String sourceDDL = "CREATE TABLE shipments (\n" +
                "  id INT,\n" +
                "  age INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'postgres-cdc',\n" +
                "  'hostname' = '192.168.119.128',\n" +
                "  'port' = '5432',\n" +
                "  'username' = 'postgres',\n" +
                "  'password' = 'postgres',\n" +
                "  'database-name' = 'postgres',\n" +
                "  'schema-name' = 'public',\n" +
                "  'table-name' = 'cdc_pg_source'\n" +
                ")";


        //执行source表ddl
        tableEnv.executeSql(sourceDDL);

        String query = "SELECT * FROM shipments";
        Table table = tableEnv.sqlQuery(query);

        tableEnv.toRetractStream(table, Row.class);
    }
}
