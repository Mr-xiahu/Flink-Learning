package cn.xhjava.flink.table.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;

/**
 * @author Xiahu
 * @create 2021/4/2
 */
public class HbaseDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("CREATE TABLE hbase_user_behavior(\n" +
                "rowkey STRING,\n" +
                "info ROW<user_id STRING,mt_wm_poi_id STRING,shop_name STRING,source STRING,platform STRING,create_time STRING,dt STRING,hr STRING,mm STRING>,\n" +
                "PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'hbase-2.2',\n" +
                "'table-name' = 'test:hbase_user_behavior',\n" +
                "'zookeeper.quorum' = '192.168.0.115:2181',\n" +
                "'zookeeper.znode.parent' = '/hbase'\n" +
                ")");

        Table table = tableEnv.sqlQuery("select rowkey from hbase_user_behavior");
        // 查询的结果
        tableEnv.toAppendStream(table, Row.class).print();

        //插入数据
        tableEnv.executeSql("INSERT INTO hbase_user_behavior\n" +
                "SELECT '2000000000', ROW('1', '1', '1') as info");

        //env.execute();

    }
}
