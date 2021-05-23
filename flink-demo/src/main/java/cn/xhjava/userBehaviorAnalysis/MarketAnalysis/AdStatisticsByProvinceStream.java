package cn.xhjava.userBehaviorAnalysis.MarketAnalysis;

import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions.AdCountAgg;
import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions.AdCountResult;
import cn.xhjava.userBehaviorAnalysis.MarketAnalysis.functions.FilterBlackListUser;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.AdClickEvent;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.AdCountViewByProvince;
import cn.xhjava.userBehaviorAnalysis.domain.marketAnalysis.BlackListUserWarning;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.OutputTag;


/**
 * 页面广告统计分析,不同省份的用户对不同广告的点击量
 */
public class AdStatisticsByProvinceStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据
        DataStream<AdClickEvent> adClickEventStream = env.readTextFile("F:\\git\\Flink-Learning\\flink-demo\\src\\main\\resources\\AdClickLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<AdClickEvent>(new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(AdClickEvent element) {
                        return element.getTimestamp();
                    }
                }));

        // 2. 对同一个用户点击同一个广告的行为进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventStream
                /*.keyBy(new KeySelector<AdClickEvent, Tuple>() {
                    @Override
                    public Tuple getKey(AdClickEvent value) throws Exception {
                        return new Tuple2<Long, Long>(value.getUserId(), value.getAdId());
                    }
                })*/
                // 基于用户id和广告id做分组
                .keyBy("userId","adId")
                .process(new FilterBlackListUser(100));


        // 3. 基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream
                .keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))     // 定义滑窗，5分钟输出一次
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();
        filterAdClickStream
                .getSideOutput(new OutputTag<BlackListUserWarning>("blacklist") {})
                .printToErr("blacklist-user");

        env.execute("ad count by province job");
    }
}
