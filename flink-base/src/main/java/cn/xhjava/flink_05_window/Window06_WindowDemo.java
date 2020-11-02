package cn.xhjava.flink_05_window;

import cn.xhjava.datasource.DataSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class Window06_WindowDemo {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        //设置运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取参数
        DataStream<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WindowWordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            //没有就使用默认
            text = env.fromElements(DataSource.WORDS);
        }

        final int windowSize = params.getInt("window", 5);
        final int slideSize = params.getInt("slide", 1);
        text
                .flatMap(new Window06_WindowDemo.Tokenizer())
                .keyBy(0)
                .countWindow(windowSize, slideSize)
                .sum(1);

        env.execute();

    }


    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
