package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.main;

import cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions.PvCountAgg;
import cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions.PvCountResult;
import cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions.TotalPvCount;
import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import cn.xhjava.userBehaviorAnalysis.domain.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import java.util.Random;

/**
 * @author Xiahu
 * @create 2021-05-17
 */
public class PageViewStream {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 2. 读取数据，创建DataStream

        DataStream<String> inputStream = env.readTextFile("D:\\code\\github\\Flink-Learning\\flink-demo\\src\\main\\resources\\UserBehavior.csv");

        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarksAdapter.Strategy<UserBehavior>(
                                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        /*SingleOutputStreamOperator<Tuple2<Long, Long>> sum = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
                .map(new MapFunction<UserBehavior, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<>(value.getItemId(), 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple2<Long, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                // 按商品ID分组
                .window(TumblingEventTimeWindows.of(Time.hours(1)))    // 开1小时滚动窗口
                .sum(1);
        sum.printToErr();*/

        //并行任务改进，设计随机key，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 将各分区数据汇总起来
        DataStream<PageViewCount> pvResultStream = pvStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());
//                .sum("count");

        pvResultStream.printToErr();
        env.execute("pv count job");
    }
}
