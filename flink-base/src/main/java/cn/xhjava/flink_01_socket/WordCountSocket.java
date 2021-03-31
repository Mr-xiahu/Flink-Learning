package cn.xhjava.flink_01_socket;

import cn.xhjava.util.ParameterToolUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * 数据源使用socket funcation
 * <p>
 * ParameterTool 的使用方法 : --socket.host 192.168.0.113 --socket.port 8989
 */
public class WordCountSocket {
    public static void main(String[] args) throws Exception {
        //1.初始化flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置全局配置参数
        /*ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        Map<String, String> param = env.getConfig().getGlobalJobParameters().toMap();

        String host = param.get("socket.host");
        Integer port = Integer.valueOf(param.get("socket.port"));
        String appName = param.get("socket.name");*/

        //3.添加数据源
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.0.113", 8889);
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] field = value.split("\\W+");
                for (String word : field) {
                    out.collect(new Tuple2<String, Long>(word, 1l));
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        }).printToErr();


        env.execute();
    }
}
