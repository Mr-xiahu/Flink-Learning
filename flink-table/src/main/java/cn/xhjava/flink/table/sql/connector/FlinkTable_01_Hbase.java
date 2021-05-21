package cn.xhjava.flink.table.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Xiahu
 * @create 2021/4/2
 * <p>
 * Table API 向hbase put数据(查询功能暂时没有调通)
 */
public class FlinkTable_01_Hbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("CREATE TABLE hbase_user_behavior(\n" +
                "rowkey STRING,\n" +
                "info ROW<man STRING,small STRING,yellow STRING>,\n" +
                "PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'hbase-2.2',\n" +
                "'table-name' = 'test:hbase_user_behavior',\n" +
                "'zookeeper.quorum' = '192.168.0.115:2181',\n" +
                "'zookeeper.znode.parent' = '/hbase'\n" +
                ")");


        //插入数据
        /*tableEnv.executeSql("INSERT INTO hbase_user_behavior\n" +
                "SELECT '2000000000', ROW('2', '2', '2') as info");*/
        Table query = tableEnv.sqlQuery("select rowkey from hbase_user_behavior limit 10");


        /*tableEnv.toAppendStream(query, Row.class).print();*/

        //env.execute();

    }
}
