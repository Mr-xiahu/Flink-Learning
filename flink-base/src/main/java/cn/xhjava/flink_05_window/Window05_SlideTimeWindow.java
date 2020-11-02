package cn.xhjava.flink_05_window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class Window05_SlideTimeWindow {
    public static void main(String[] args) throws Exception {
        /**
         * 下面这段代码: WordCount
         *      1.使用socketTextStream作为dataSource,接收数据
         *      2.使用flatMap将接收的word封装为成元组Tuple2<String,Integre>
         *      3.根据word进行分组,将word相同的分为同一组
         *      4.使用时间窗口:timeWindow,每隔15s,统计过去30s的word数量之和并打印
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
                }).keyBy(0)
                .timeWindow(Time.seconds(30), Time.seconds(15))
                .sum(1).print();
        env.execute();
    }
}
