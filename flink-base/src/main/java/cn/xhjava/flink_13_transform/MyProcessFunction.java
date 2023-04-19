package cn.xhjava.flink_13_transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2023/3/27 0027
 */
@Slf4j
public class MyProcessFunction<I> extends MyAbstractProcessFunction<I> implements CheckpointedFunction {
    public MyProcessFunction(Configuration conf) {

    }


    @Override
    public void processElement(I value, ProcessFunction<I, Object>.Context ctx, Collector<Object> out) throws Exception {
        String[] s = value.toString().split(" ");
        for (String s1 : s) {
            out.collect(s1);
        }
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("snapshotState ...");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("initializeState ...");
    }
}
