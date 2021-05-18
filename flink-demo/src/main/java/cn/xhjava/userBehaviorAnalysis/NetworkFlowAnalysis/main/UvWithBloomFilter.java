package cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.main;

import cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions.MyTrigger;
import cn.xhjava.userBehaviorAnalysis.NetworkFlowAnalysis.functions.UvCountResultWithBloomFliter;
import cn.xhjava.userBehaviorAnalysis.domain.PageViewCount;
import cn.xhjava.userBehaviorAnalysis.domain.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

/**
 * @ClassName: UvWithBloomFilter
 * @Description:
 * @Author: wushengran on 2020/11/16 15:28
 * @Version: 1.0
 */
public class UvWithBloomFilter {
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
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFliter());

        uvStream.print();

        env.execute("uv count with bloom filter job");
    }


}
