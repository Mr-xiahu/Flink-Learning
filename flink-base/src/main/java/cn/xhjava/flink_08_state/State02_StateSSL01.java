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
 * 构建StateTTL
 */
public class State02_StateSSL01 extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("xiahu", String.class);

        //构建StateTtlConfig
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))//不能缺,代表状态存活时间
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//哪种操作会使状态更新
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//是否在读取访问时返回过期值
                .build();
        stateDescriptor.enableTimeToLive(ttlConfig);
    }
}
