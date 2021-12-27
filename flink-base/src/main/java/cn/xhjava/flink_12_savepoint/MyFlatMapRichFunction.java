package cn.xhjava.flink_12_savepoint;

import lombok.val;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2021/12/27 0027
 */
public class MyFlatMapRichFunction extends RichFlatMapFunction<String, Tuple2<String, Integer>> implements CheckpointedFunction {

    private ListState listState;
    private List<String> listValue = new ArrayList<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        for (String word : value.split("\\s")) {
            listValue.add(word);
            System.out.println(word);
            out.collect(new Tuple2<>(word, 1));
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.addAll(listValue);
        listValue.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        ListStateDescriptor stateDescriptor = new ListStateDescriptor(
                "multiple-parquet-write-status",
                TypeInformation.of(String.class));
        listState = stateStore.getListState(stateDescriptor);
        List<String> stateVaue = (List<String>) listState.get();
        if (context.isRestored()) {
            for (String line : stateVaue) {
                System.out.println("checkpoint: " + line);
                listValue.add(line);
            }
        }


    }
}
