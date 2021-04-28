package cn.xhjava.flink_05_window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class Window02_TimeWindow {
    public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用socketTextStream作为dataSource,接收数据
         *      2.使用flatMap将接收的word封装为成元组Tuple2<String,Integre>
         *      3.根据word进行分组,将word相同的分为同一组
         *      4.使用时间窗口:timeWindow,每隔30s做一个sum,并打印
         */

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3.添加数据源
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.0.113", 8081);
        //使用算子解析
        dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] split = s.split("\\W+");
                        for (String sttr : split) {
                            collector.collect(new Tuple2<>(sttr, 1));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f1;
                    }
                })
                //处理时间窗口
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //事件时间窗口
                //.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(1).print();
        env.execute();
    }
}
