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
 * 清除过期State
 */
public class State02_StateSSL02 extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Long>> collector) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("xiahu", String.class);

        /**
         * 默认情况下,过期值只有在显式读出时才会被删除,例如通过调用 ValueState.value().
         * 这意味着只要不 显示读出,state会越来愈大.
         * 你可以在进行sum.value()时获取完整的状态,然后执行sum.clean()去清理过期的状态
         */


        //构建StateTtlConfig
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .cleanupFullSnapshot()//清除过去快照(状态)
                .build();

        /**
         * 此配置不适用于 RocksDB 状态后端中的增量 checkpoint.
         * 对于现有的 Job,可以在 StateTtlConfig 中随时激活或停用此清理策略,例如,从保存点重启后。
         */

        stateDescriptor.enableTimeToLive(ttlConfig);
    }
}
