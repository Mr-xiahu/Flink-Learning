package cn.xhjava.userBehaviorAnalysis.MarketAnalysis;

import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions.MarketingStatisticsAgg;
import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions.MarketingStatisticsResult;
import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.util.SimulatedMarketingUserBehaviorSource;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.ChannelPromotionCount;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

/**
 * 不分渠道的APP 市场推广统计
 */
public class AppMarketingStatisticsStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<MarketingUserBehavior>(new BoundedOutOfOrdernessTimestampExtractor<MarketingUserBehavior>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                }));

        // 2. 开窗统计总量
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                })
                .keyBy((value) -> {
                    return value.f0;
                })
                // 定义滑窗
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());


        resultStream.print();

        env.execute("app marketing by channel job");
    }

}
