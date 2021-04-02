package cn.xhjava.flink.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2021/4/1
 * <p>
 * 创建TableEnvironment
 */
public class Flink_Table_01 {
    public static void main(String[] args) {
        //FLINK STREAMING QUERY
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        // FLINK BATCH QUERY
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        //BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

        // BLINK STREAMING QUERY
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        // BLINK BATCH QUERY
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
    }
}
