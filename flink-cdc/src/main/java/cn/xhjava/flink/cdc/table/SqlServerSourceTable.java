package cn.xhjava.flink.cdc.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * @author Xiahu
 * @create 2022/6/27 0027
 */
public class SqlServerSourceTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //拼接souceDLL
        String sourceTable = "CREATE TABLE orders (\n" +
                "    id INT,\n" +
                "    name varchar"+
                ") WITH (\n" +
                "    'connector' = 'sqlserver-cdc',\n" +
                "    'hostname' = '192.168.0.36',\n" +
                "    'port' = '1433',\n" +
                "    'username' = 'sa',\n" +
                "    'password' = 'P@ssw0rd',\n" +
                //"    'format' = 'ogg-json'," +  format  Unsupported options found for 'sqlserver-cdc'.
                "    'database-name' = 'hid0101_cache_his_xh',\n" +
                "    'schema-name' = 'dbo'," +
                "    'table-name' = 'test_2,table_3'" +
                ")";



        /*
        connector
        database-name
        hostname
        password
        port
        property-version
        scan.startup.mode
        schema-name
        server-time-zone
        table-name
        username
        */


        //执行source表ddl
        tableEnv.executeSql(sourceTable);
        tableEnv.executeSql("SELECT * FROM orders").print();
        /*Table table = tableEnv.sqlQuery("SELECT * FROM orders");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();*/
    }
}
