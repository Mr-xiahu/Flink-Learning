package cn.xhjava.flink_00_wordcount;

import cn.xhjava.datasource.DataSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class WordCountStreaming {
    public static void main(String[] args) throws Exception {
        //1.初始化Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.加载配置
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        //3.添加数据源
        env.fromElements(DataSource.WORDS)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] field = line.split("\\W+");
                        for (String word : field) {
                            collector.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
            }
        }).print();

        env.execute("Word Count By Stream");
    }
}
