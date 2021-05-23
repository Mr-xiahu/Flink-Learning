package cn.xhjava.userBehaviorAnalysis.MarketAnalysis;

import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions.MarketingCountAgg;
import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions.MarketingCountResult;
import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.util.SimulatedMarketingUserBehaviorSource;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.ChannelPromotionCount;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.MarketingUserBehavior;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

/**
 * 分渠道的APP 市场推广统计
 */
public class AppMarketingByChannelStream {
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

        // 2. 分渠道开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))    // 定义滑窗
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print();

        env.execute("app marketing by channel job");
    }
}
