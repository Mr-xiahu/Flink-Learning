package cn.xhjava.flink_08_state;

import cn.xhjava.flink_08_state.funcation.CountWindow;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class State01_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L),
                Tuple2.of(1L, 4L),
                Tuple2.of(1L, 2L)
        )
                .keyBy(0)
                .flatMap(new CountWindow())
                .print();

        env.execute();
    }
}
