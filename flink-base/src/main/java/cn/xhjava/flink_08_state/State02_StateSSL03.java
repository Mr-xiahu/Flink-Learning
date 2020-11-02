package cn.xhjava.flink_08_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/11/2
 *
 * 后台激活清理State
 */
public class State02_StateSSL03 extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("xiahu", String.class);

        /**
         * 如果使用的后端支持以下选项,则会激活 StateTtlConfig 中的默认后台清理.
         */

        //构建StateTtlConfig
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .cleanupInBackground()
                .build();

        /**
         * 要在后台对某些特殊清理进行更精细的控制,可以按照下面的说明单独配置它.
         * 目前,堆状态后端依赖于增量清理,RocksDB 后端使用压缩过滤器进行后台清理
         */
        stateDescriptor.enableTimeToLive(ttlConfig);
    }
}
