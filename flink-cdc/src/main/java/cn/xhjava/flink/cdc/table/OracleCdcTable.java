package cn.xhjava.flink.cdc.table;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/10/27 0027
 */
public class OracleCdcTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /*String sql = "CREATE TABLE oracle_products_cdc (\n"
                + " ID INT NOT NULL,\n"
                + " NAME STRING,\n"
                + " ADDRESS STRING,\n"
                + " PHONE_NUMBER STRING,\n"
                + " LASTUPDATADTTM STRING,\n"
                + " PRIMARY KEY (ID) NOT ENFORCED\n"
                + ") WITH ("
                + " 'connector' = 'oracle-cdc',\n"
                + " 'hostname' = '192.168.0.67',\n"
                + " 'port' = '1521',\n"
                + " 'username' = 'klbr',\n"
                + " 'password' = 'klbr',"
                + " 'database-name' = 'dbcenter',\n"
                + " 'schema-name' = 'klbr',\n"
                + " 'table-name' = 'PRODUCT',\n"
                // + " 'scan.incremental.snapshot.enabled' = 'false',\n"
                + " 'debezium.log.mining.strategy' = 'online_catalog',\n"
                + " 'debezium.database.connection.adapter' = 'xstream',\n"
                + " 'debezium.database.out.server.name' = 'dbtestout',\n"
                + " 'debezium.log.mining.continuous.mine' = 'true'\n"
                // + " 'log.mining.batch.size.max' = '500',\n"
                // + " 'log.mining.archive.log.only.scn.poll.interval.ms' ='500' "
                + ")";*/

        String sql = "CREATE TABLE oracle_products_cdc (\n"
                + " ID INT NOT NULL,\n"
                + " NAME STRING,\n"
                + " ADDRESS STRING,\n"
                + " PHONE_NUMBER STRING,\n"
                + " LASTUPDATADTTM STRING,\n"
                + " PRIMARY KEY (ID) NOT ENFORCED\n"
                + ") WITH ("
                + " 'connector' = 'oracle-cdc',\n"
                + " 'hostname' = '192.168.0.67',\n"
                + " 'port' = '1521',\n"
                + " 'username' = 'xstrm',\n"
                + " 'password' = 'xstrm',"
                + " 'database-name' = 'dbcenter',\n"
                + " 'schema-name' = 'klbr',\n"
                + " 'table-name' = 'PRODUCT',\n"
                // + " 'scan.incremental.snapshot.enabled' = 'false',\n"
                + " 'debezium.log.mining.strategy' = 'online_catalog',\n"
                + " 'debezium.database.connection.adapter' = 'xstream',\n"
                + " 'debezium.database.out.server.name' = 'dbtestout',\n"
                + " 'debezium.log.mining.continuous.mine' = 'true'\n"
                // + " 'log.mining.batch.size.max' = '500',\n"
                // + " 'log.mining.archive.log.only.scn.poll.interval.ms' ='500' "
                + ")";


        tableEnv.executeSql(sql);
        tableEnv.executeSql("select * from oracle_products_cdc").print();


        env.execute("Print MySQL Snapshot + Binlog");
    }
}
