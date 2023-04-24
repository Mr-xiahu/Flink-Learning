package cn.xhjava.flink.cdc.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class PostgresCdcTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //拼接souceDLL
        String sourceDDL = "CREATE TABLE shipments (" +
                "  id INT," +
                "  age INT," +
                "  name STRING" +
                ") WITH (" +
                "  'connector' = 'postgres-cdc'," +
                "  'hostname' = '192.168.119.128'," +
                "  'port' = '5432'," +
                "  'username' = 'postgres'," +
                "  'password' = 'postgres'," +
                "  'decoding.plugin.name' = 'wal2json'," +
                "  'database-name' = 'postgres'," +
                "  'schema-name' = 'public'," +
                "  'table-name' = 'cdc_pg_source'" +
                ")";



//        //拼接souceDLL
//        String sourceDDL = "CREATE TABLE shipments (" +
//                "  id INT," +
//                "  age INT," +
//                "  name STRING" +
//                ") WITH (" +
//                "  'connector' = 'postgres-cdc'," +
//                "  'hostname' = '192.168.119.128'," +
//                "  'port' = '5432'," +
//                "  'username' = 'postgres'," +
//                "  'password' = 'postgres'," +
//                "  'decoding.plugin.name' = 'wal2json'," +
//                "  'database-name' = 'postgres'," +
//                "  'schema-name' = 'public'," +
//                "  'table-name' = 'cdc_pg_source'" +
//                ")";
//
//
//        in sout form

        //执行source表ddl
        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql("SELECT * FROM shipments").print();


        env.execute();
    }
}
