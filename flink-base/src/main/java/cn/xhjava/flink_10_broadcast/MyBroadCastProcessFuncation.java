package cn.xhjava.flink_10_broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.xml.MetaData;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/11/9
 */
public class MyBroadCastProcessFuncation extends BroadcastProcessFunction<String, List<MetaData>, String> {
    MapStateDescriptor<String, MetaData> metadata = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        metadata = new MapStateDescriptor<>("metadata", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(MetaData.class));
    }

    /**
     * 注意：
     *      当kafkaStream与broadcastStream  connect 进来时,首先执行processElement().
     *      此时你只能获取kafkaSteam数据,无法获取broadcastStream,两者无法关联起来.后面才会执行processBroadcastElement()
     *      如果你想在broadcastStream加载完毕后,去处理kafkaSteam.
     *      https://segmentfault.com/q/1010000022940925
     *      主要逻辑: 将kafkaSteam 的数据内容缓存到State,等待broadcastStream加载完毕后,再去执行相关逻辑
     */

    //处理广播流数据
    @Override
    public void processBroadcastElement(List<MetaData> value, Context ctx, Collector<String> out) throws Exception {
        if (value == null || value.size() == 0) {
            return;
        }
        BroadcastState<String, MetaData> alertRuleBroadcastState = ctx.getBroadcastState(metadata);
        for (int i = 0; i < value.size(); i++) {
            alertRuleBroadcastState.put(value.toString(), value.get(i));
        }
    }


    //处理非广播流数据
    @Override
    public void processElement(String msg, ReadOnlyContext ctx, Collector<String> out) throws Exception {

        ReadOnlyBroadcastState<String, MetaData> broadcastState = ctx.getBroadcastState(metadata);
        //解析流成为对象
        //MetaData newMetaData = NuwaConsumerHelper.parseMsg(msg);
        Iterator<Map.Entry<String, MetaData>> iterator = broadcastState.immutableEntries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, MetaData> next = iterator.next();
            System.err.println("----->" + next.getKey());
        }
    }


}
