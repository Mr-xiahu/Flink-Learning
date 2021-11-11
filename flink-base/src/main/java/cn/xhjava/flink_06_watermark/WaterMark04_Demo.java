package cn.xhjava.flink_06_watermark;

import cn.xhjava.domain.Event;
import cn.xhjava.flink_06_watermark.watermark.PunctuatedWatermark;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/2
 * <p>
 * flink 1.12.2 设置watermark
 */
public class WaterMark04_Demo {


    public static void main(String[] args) throws Exception {
        //1.初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置全局配置参数
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        Map<String, String> param = env.getConfig().getGlobalJobParameters().toMap();

        String host = param.get("socket.host");
        Integer port = Integer.valueOf(param.get("socket.port"));
        String appName = param.get("socket.name");

        //设置为eventtime事件类型
        //TODO disable watermarks
        //watermark 默认使用的是事件时间
        env.getConfig().setAutoWatermarkInterval(10);


        //3.添加数据源
        DataStream<String> dataStream = env.socketTextStream(host, port);
        DataStream<Event> flatMapDS = dataStream
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String value, Collector<Event> out) throws Exception {
                        String[] split = value.split("\\W+");
                        for (String wrod : split) {
                            out.collect(new Event(1, wrod, new Date().getTime()));
                        }
                    }
                });

        //TODO 设置watermark
        flatMapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimeStamp();
                            }
                        })
        ).keyBy(new KeySelector<Event, Integer>() {
            @Override
            public Integer getKey(Event value) throws Exception {
                return value.getId();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        env.execute(appName);
    }
}
