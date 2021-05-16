package cn.xhjava.userBehaviorAnalysis.HotItemsAnalysis;


import cn.xhjava.userBehaviorAnalysis.HotItemsAnalysis.functions.ItemCountAgg;
import cn.xhjava.userBehaviorAnalysis.HotItemsAnalysis.functions.TopNHotItems;
import cn.xhjava.userBehaviorAnalysis.HotItemsAnalysis.functions.WindowItemCountResult;
import cn.xhjava.userBehaviorAnalysis.domain.ItemViewCount;
import cn.xhjava.userBehaviorAnalysis.domain.UserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author XiaHu
 * @create 2021/5/16
 */
public class HotItemsAnalysisStream {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置事件事件(默认就是事件时间)
        env.getConfig().setAutoWatermarkInterval(200);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("F:\\git\\Flink-Learning\\flink-demo\\src\\main\\resources\\UserBehavior.csv");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

//        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));


        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<UserBehavior>(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));


        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
                .keyBy(new KeySelector<UserBehavior, Long>() {
                    @Override
                    public Long getKey(UserBehavior value) throws Exception {
                        return value.getItemId();
                    }
                })    // 按商品ID分组
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))// 开滑窗
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());


        // 5. 收集同一窗口的所有商品count数据，排序输出top n
        DataStream<String> resultStream = windowAggStream
                .keyBy(new KeySelector<ItemViewCount, Long>() {
                    @Override
                    public Long getKey(ItemViewCount value) throws Exception {
                        return value.getWindowEnd();
                    }
                })    // 按照窗口分组
                .process(new TopNHotItems(5));   // 用自定义处理函数排序取前5


        resultStream.print();

        env.execute("hot items analysis");
    }


}