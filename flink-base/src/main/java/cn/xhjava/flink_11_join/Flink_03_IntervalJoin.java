package cn.xhjava.flink_11_join;

import cn.xhjava.domain.UserBrowseLog;
import cn.xhjava.domain.UserClickLog;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2020/11/27
 */
@Slf4j
public class Flink_03_IntervalJoin {
    public static void main(String[] args) throws Exception {
        String kafkaBootstrapServers = "node1:9092";
        String browseTopic = "browse_topic";
        String clickTopic = "click_topic";
        String groupid = "interval-join-test";

        //2、设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //3、添加Kafka数据源
        // 浏览流
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers", kafkaBootstrapServers);
        browseProperties.put("group.id", groupid);
        DataStream<UserBrowseLog> browseStream = env
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties).setStartFromEarliest())
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new BrowseBoundedOutOfOrdernessTimestampExtractor(Time.seconds(0)));

        // 点击流
        Properties clickProperties = new Properties();
        clickProperties.put("bootstrap.servers", kafkaBootstrapServers);
        clickProperties.put("group.id", groupid);
        DataStream<UserClickLog> clickStream = env
                .addSource(new FlinkKafkaConsumer<>(clickTopic, new SimpleStringSchema(), clickProperties).setStartFromEarliest())
                .process(new ClickKafkaProcessFunction())
                .assignTimestampsAndWatermarks(new ClickBoundedOutOfOrdernessTimestampExtractor(Time.seconds(0)));

        /*browseStream.printToErr();
        clickStream.printToErr();*/

        //4、Interval Join
        //每个用户的点击Join这个用户最近10分钟内的浏览
        clickStream
                .keyBy("userID")
                .intervalJoin(browseStream.keyBy("userID"))
                // 时间间隔,设定下界和上界
                //上界: 当前EventTime, 下界: 10分钟前
                .between(Time.minutes(0), Time.minutes(10))
                // 自定义ProcessJoinFunction 处理Join到的元素
                .process(new ProcessJoinFunction<UserClickLog, UserBrowseLog, String>() {
                    @Override
                    public void processElement(UserClickLog left, UserBrowseLog right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + " =Interval Join=> " + right);
                    }
                })
                .printToErr();

        env.execute();

    }

    /**
     * 解析Kafka数据
     */
    static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {
                UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);
                if (log != null) {
                    out.collect(log);
                }
            } catch (Exception ex) {
                log.error("解析Kafka数据异常...", ex);
            }
        }
    }

    /**
     * 解析Kafka数据
     */
    static class ClickKafkaProcessFunction extends ProcessFunction<String, UserClickLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserClickLog> out) throws Exception {
            try {
                UserClickLog log = JSON.parseObject(value, UserClickLog.class);
                if (log != null) {
                    out.collect(log);
                }
            } catch (Exception ex) {
                log.error("解析Kafka数据异常...", ex);
            }
        }
    }

    /**
     * 提取时间戳生成水印
     */
    static class BrowseBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog> {

        BrowseBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
            return dateTime.getMillis();
        }
    }

    /**
     * 提取时间戳生成水印
     */
    static class ClickBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserClickLog> {

        ClickBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserClickLog element) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
            getCurrentWatermark();
            return dateTime.getMillis();
        }
    }

}
