package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.Student;
import cn.xhjava.source.StreamDataSource01;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * 需要结合watchmark,window一起使用
 */
public class Operator10_WindowJion {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        int windowSize = 10;
        long delay = 5100L;

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 设置数据源
        DataStream<Tuple3<String, String, Long>> leftSource = env.addSource(new StreamDataSource01()).name("Demo Source");
        DataStream<Tuple3<String, String, Long>> rightSource = env.addSource(new StreamDataSource01()).name("Demo Source");

        // 设置水位线
        DataStream<Tuple3<String, String, Long>> leftStream = leftSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                }
        );

        DataStream<Tuple3<String, String, Long>> rigjhtStream = rightSource.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                }
        );

        // join 操作
        leftStream
                .join(rigjhtStream)
                .where(new KeySelector<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>>() {
                    @Override
                    public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) {
                        return new Tuple5<>(first.f0, first.f1, second.f1, first.f2, second.f2);
                    }
                }).print();


        //3.执行
        env.execute("flink operator WindowJion");
    }
}
