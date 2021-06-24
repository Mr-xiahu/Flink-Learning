package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.main;

import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import cn.xhjava.userBehaviorAnalysis.domain.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * UV
 */
public class UniqueVisitorStream {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("D:\\code\\github\\Flink-Learning\\flink-demo\\src\\main\\resources\\UserBehavior.csv");

        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<UserBehavior>(
                        new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(0)) {
                            @Override
                            public long extractTimestamp(UserBehavior element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }));

        // 开窗统计uv值
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new UvCountResult());

        uvStream.printToErr();

        env.execute("uv count job");
    }

    // 实现自定义全窗口函数
    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义一个Set结构，保存窗口中的所有userId，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior ub : values)
                uidSet.add(ub.getUserId());
            out.collect(new PageViewCount("uv", window.getEnd(), (long) uidSet.size()));
        }
    }
}
