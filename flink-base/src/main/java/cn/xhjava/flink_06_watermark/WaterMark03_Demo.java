package cn.xhjava.flink_06_watermark;

import cn.xhjava.domain.Event;
import cn.xhjava.flink_06_watermark.watermark.PunctuatedWatermark;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/2
 * <p>
 * 延迟数据处理
 */
public class WaterMark03_Demo {


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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<Event> lateDataTag = new OutputTag<Event>("late") {
        };

        //3.添加数据源
        DataStreamSource<String> dataStream = env.socketTextStream(host, port);
        dataStream
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String value, Collector<Event> out) throws Exception {
                        String[] split = value.split("\\W+");
                        for (String wrod : split) {
                            out.collect(new Event(1, wrod, new Date().getTime()));
                        }
                    }
                })
                //添加水印
                .assignTimestampsAndWatermarks(new PunctuatedWatermark())
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.milliseconds(2))//额外允许的延迟时间(watermark 内部可以手动设置延迟时间,此处如果设置,总延迟时间= 此处+watermark处)
                .sideOutputLateData(lateDataTag)
                .process(new ProcessWindowFunction<Event, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
                        Iterator<Event> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            out.collect(iterator.next());
                        }
                    }
                }).printToErr();

        //处理延迟数据
        DataStream<Event> sideOutput = dataStream.getSideOutput(lateDataTag);
        sideOutput.print();
        env.execute(appName);
    }
}
