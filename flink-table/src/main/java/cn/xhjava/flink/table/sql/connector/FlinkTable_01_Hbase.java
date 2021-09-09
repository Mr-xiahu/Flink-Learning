package cn.xhjava.flink.table.sql.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Xiahu
 * @create 2021/4/2
 * <p>
 * Table API 向hbase put数据(查询功能暂时没有调通)
 */
public class FlinkTable_01_Hbase {
    public static void main(String[] args) throws Exception {

        /**
         * 方式一: 该方式用于 实时流表关键维度静态表,直接下面demo执行,无法打印结果,需要与主表进行join使用
         */
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.executeSql("CREATE TABLE dformfiled(\n" +
//                "rowkey STRING,\n" +
//                "info ROW<formid STRING>,\n" +
//                "PRIMARY KEY (rowkey) NOT ENFORCED\n" +
//                ") WITH (\n" +
//                "'connector' = 'hbase-2.2',\n" +
//                "'table-name' = 'hid0101_cache_his_dhcapp_nemrforms:dformfiled',\n" +
//                "'zookeeper.quorum' = '192.168.0.115:2181',\n" +
//                "'zookeeper.znode.parent' = '/hbase'\n" +
//                ")");
//
//        Table table = tableEnv.sqlQuery("select a.*,b.* from tmp a left join dformfiled b on a.key = b.rowkey and b.formid='550'");
//
//        tableEnv.toRetractStream(table,Row.class).print();
//        env.execute();


        //todo 如下:
        /*StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> kafkaSource = env.addSource(source);
        DataStream<TmpTable> flatMap = kafkaSource.filter(new FilterTableFunction(bean))
                .flatMap(new FlatMapFunction(bean));

        //主表
        Table tmpTable = tableEnv.fromDataStream(flatMap, StrUtil.list2Str(bean.getQueryColumns()));
        tableEnv.createTemporaryView("tmp", tmpTable);

        tableEnv.executeSql("CREATE TABLE dformfiled(\n" +
                "rowkey STRING,\n" +
                "info ROW<formid STRING>,\n" +
                "PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'hbase-2.2',\n" +
                "'table-name' = 'hid0101_cache_his_dhcapp_nemrforms:dformfiled',\n" +
                "'zookeeper.quorum' = '192.168.0.115:2181',\n" +
                "'zookeeper.znode.parent' = '/hbase'\n" +
                ")");


        //查询数据
        Table table = tableEnv.sqlQuery("select a.*,b.* from tmp a left join dformfiled b on a.key = b.rowkey and b.formid='550'");
        tableEnv.toRetractStream(table,Row.class).print();
        env.execute();*/


        /**
         * 方式二：使用下面代码可直接查询结果,但是在打印时会报错,暂时还未解决
         */
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(blinkBatchSettings);


        tableEnv.executeSql("CREATE TABLE dformfiled(\n" +
                "rowkey STRING,\n" +
                "info ROW<text5 STRING,text6 STRING,text7 STRING,text8 STRING,text9 STRING,textarea2 STRING,rowkey STRING>,\n" +
                "PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'hbase-2.2',\n" +
                "'table-name' = 'hid0101_cache_his_dhcapp_nemrforms:dformfiled',\n" +
                "'zookeeper.quorum' = '192.168.0.115:2181',\n" +
                "'zookeeper.znode.parent' = '/hbase'\n" +
                ")");


        //插入数据
        Table query = tableEnv.sqlQuery("select rowkey from dformfiled limit 10");

        query.execute().print();

    }
}
