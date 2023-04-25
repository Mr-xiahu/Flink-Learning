package cn.xhjava.flink_15_restart_strategy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

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

        /**
         * 重启策略设置
         */
        //
        /**
         *
         * 固定延迟重启策略:尝试给定次数重新启动作业。如果超过最大尝试次数，则作业失败。
         * 在两次连续重启尝试之间，会有一个固定的延迟等待时间。
         *
         * 通过在flink-conf.yaml中配置参数：
         *         restart-strategy: fixed-delay              # fixed-delay:固定延迟策略
         *         restart-strategy.fixed-delay.attempts: 5   # 尝试5次，默认Integer.MAX_VALUE
         *         restart-strategy.fixed-delay.delay: 10s    # 设置延迟时间10s，默认为 akka.ask.timeout时间
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(10)));


        /**
         * 故障率重启策略:在故障后重新作业，当设置的故障率（failure rate）超过每个时间间隔的故障时，作业最终失败。
         * 在两次连续重启尝试之间，重启策略延迟等待一段时间。
         *
         * 通过在flink-conf.yaml中配置参数：
         *      restart-strategy: failure-rate                          # 设置重启策略为failure-rate
         *      restart-strategy.failure-rate.max-failures-per-interval: 3  # 失败作业之前的给定时间间隔内的最大重启次数，默认1
         *      restart-strategy.failure-rate.failure-rate-interval: 5min   # 测量故障率的时间间隔。默认1min
         *      restart-strategy.failure-rate.delay: 10s                 # 两次连续重启尝试之间的延迟，默认akka.ask.timeout时间
         * 失败后，5分钟内重启3次（每次重启间隔10s），如果第3次还是失败，则任务最终是失败，不再重启。
         */
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(2, TimeUnit.SECONDS),
                Time.of(3, TimeUnit.SECONDS))
        );

        /**
         * 第一次失败后就最终失败，不再重启
         *
         * 通过在flink-conf.yaml中配置参数：
         *      restart-strategy: none
         */
        env.setRestartStrategy(RestartStrategies.noRestart());


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
