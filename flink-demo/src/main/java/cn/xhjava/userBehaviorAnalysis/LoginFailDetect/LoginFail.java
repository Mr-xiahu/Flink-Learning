package cn.xhjava.userBehaviorAnalysis.LoginFailDetect;


import cn.xhjava.userBehaviorAnalysis.LoginFailDetect.functions.LoginFailDetectWarning;
import cn.xhjava.userBehaviorAnalysis.domain.LoginEvent;
import cn.xhjava.userBehaviorAnalysis.domain.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 登陆异常检测
 */
public class LoginFail {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据
        DataStream<LoginEvent> loginEventStream = env.readTextFile("F:\\git\\Flink-Learning\\flink-demo\\src\\main\\resources\\LoginLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<LoginEvent>(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        // 自定义处理函数检测连续登录失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute("login fail detect job");
    }
}
