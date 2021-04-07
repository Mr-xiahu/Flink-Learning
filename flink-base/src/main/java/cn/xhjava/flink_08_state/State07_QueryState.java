package cn.xhjava.flink_08_state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class State07_QueryState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, Long>> dataStream = env.fromElements(
                new Tuple2<Integer, Long>(1, 1l),
                new Tuple2<Integer, Long>(2, 1l),
                new Tuple2<Integer, Long>(3, 1l),
                new Tuple2<Integer, Long>(4, 1l),
                new Tuple2<Integer, Long>(5, 1l)
        );

        // Reducing state
        ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState = new ReducingStateDescriptor(
                "xiahu",
                new MySumReduce(),
                MySumReduce.class);

        final String queryName = "xiahu";

        final QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
                dataStream.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
                    private static final long serialVersionUID = -4126824763829132959L;

                    @Override
                    public Integer getKey(Tuple2<Integer, Long> value) {
                        return value.f0;
                    }
                }).asQueryableState(queryName, reducingState);


    }
}

class MySumReduce implements ReduceFunction<Tuple2<Integer, Long>> {

    @Override
    public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> t1, Tuple2<Integer, Long> t2) throws Exception {
        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
    }
}
