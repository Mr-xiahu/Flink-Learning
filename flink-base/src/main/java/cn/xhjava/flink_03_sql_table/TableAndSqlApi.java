package cn.xhjava.flink_03_sql_table;

import cn.xhjava.domain.Item;
import cn.xhjava.source.MyStreamSource;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * Table & SQL API 使用
 */
public class TableAndSqlApi {
    public static void main(String[] args) throws Exception {
        //1.初始化flink环境
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局配置
        sEnv.getConfig().setGlobalJobParameters(parameterTool);
        //构造流式SQL查询
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(sEnv, bsSettings);

        DataStream<Item> sourceStream = sEnv
                .addSource(new MyStreamSource())
                .map(new MapFunction<Item, Item>() {
                    @Override
                    public Item map(Item value) throws Exception {
                        return value;
                    }
                });

        //分流,split 在Flink1.12.2不允许被使用,需要分流,使用：SideoutPut
        /*DataStream<Item> eventDataStream = sourceStream.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("even");


        DataStream<Item> oddDataStream = sourceStream.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("odd");*/


        //创建表的临时视图
        /*stEnv.createTemporaryView("evenTable", eventDataStream);
        stEnv.createTemporaryView("oddTable", oddDataStream);*/

        Table queryTable = stEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");
        queryTable.printSchema();

        //注册到流
        stEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, String>>() {
        })).print();

        sEnv.execute("Flink Table & SQL");
    }
}
