package cn.xhjava.flink.table.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/2
 * <p>
 * Table Api connector kafka 表
 *  kafka msg format: {"behavior":"46260480222787851673","category_id":571,"item_id":570,"ts":1617852113835,"user_id":569}
 *  查询结果:
 *      user_id                   item_id               category_id                  behavior                event_time
 *          569                       570                       571      46260480222787851673   2021-04-08T10:33:06.626
 */
public class FlinkTable_02_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("CREATE TABLE kafka_source ( \n" +
                "user_id BIGINT, \n" +
                "item_id BIGINT, \n" +
                "category_id BIGINT, \n" +
                "behavior STRING,\n" +
                "`event_time` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH ( \n" +
                "'connector' = 'kafka',  \n" +
                "'topic' = 'flink_table_connect_kafka',  \n" +
                "'scan.startup.mode' = 'earliest-offset',  \n" +
                "'properties.bootstrap.servers' = '192.168.0.113:9092',  \n" +
                "'properties.zookeeper' = '192.168.0.115:2181/cdh_kafka', \n" +
                "'format' = 'json'\n" +
                ")");


        //插入数据

        Table query = tableEnv.sqlQuery("select * from kafka_source");

        tableEnv.toAppendStream(query, Row.class).print();

        //env.execute();

    }
}
