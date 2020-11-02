package cn.xhjava.flink_06_watermark;

import cn.xhjava.domain.Event;
import cn.xhjava.flink_06_watermark.watermark.PeriodicWatermark;
import cn.xhjava.util.DateUtil;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class WaterMark01_Demo {


    public static void main(String[] args) throws Exception {
        //1.初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置全局配置参数
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        Map<String, String> param = env.getConfig().getGlobalJobParameters().toMap();

        //设置为eventtime事件类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String host = param.get("socket.host");
        Integer port = Integer.valueOf(param.get("socket.port"));
        String appName = param.get("socket.name");

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
                .assignTimestampsAndWatermarks(new PeriodicWatermark())
                .printToErr();


        env.execute(appName);
    }
}
