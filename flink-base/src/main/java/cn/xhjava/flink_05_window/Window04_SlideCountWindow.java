package cn.xhjava.flink_05_window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class Window04_SlideCountWindow {
    public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用fromElements 作为数据源
         *      2.使用flatMap过滤 f0 >5 的元组
         *      3.根据 f1 进行分组,将 f1 相同的分为同一组
         *      4.使用数量窗口:countWindow,每隔 1 个元组,将之前的 2 个元组的 f0 做一次 sum,将元组的 f0 做一次sum ,并打印
         */

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.添加数据源
        DataStreamSource<Tuple3<Integer, String, Long>> dataStream = env.fromElements(
                new Tuple3<>(1, "zhangsan", 18l),
                new Tuple3<>(2, "lisi", 17l),
                new Tuple3<>(3, "wangwu", 20l),
                new Tuple3<>(4, "zhaoliu", 25l),
                new Tuple3<>(5, "xiahu", 22l),
                new Tuple3<>(5, "xiahu", 25l),
                new Tuple3<>(6, "yangming", 21l),
                new Tuple3<>(1, "zhangsan", 18l),
                new Tuple3<>(2, "lisi", 17l),
                new Tuple3<>(3, "wangwu", 20l),
                new Tuple3<>(4, "zhaoliu", 25l),
                new Tuple3<>(5, "xiahu", 22l),
                new Tuple3<>(5, "xiahu", 25l),
                new Tuple3<>(6, "yangming", 21l)
        );
        //使用算子解析
        dataStream
                .flatMap(new FlatMapFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>>() {
                    @Override
                    public void flatMap(Tuple3<Integer, String, Long> tuple, Collector<Tuple3<Integer, String, Long>> collector) throws Exception {
                        if (tuple.f0 <= 5 && tuple.f0 >= 0) {
                            collector.collect(tuple);
                        }
                    }
                })
                .keyBy(1)
                .countWindow(2, 1)
                .sum(0).print();


        env.execute();
    }
}
