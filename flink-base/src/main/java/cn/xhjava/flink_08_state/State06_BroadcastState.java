package cn.xhjava.flink_08_state;

import cn.xhjava.domain.AlertEvent;
import cn.xhjava.domain.Rule;
import cn.xhjava.kafka.KafkaUtil;
import cn.xhjava.schema.AlertEventSchema;
import cn.xhjava.source.MySqlSource;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/11/2
 * <p>
 * 广播的数据:监控告警的通知策略规则
 */
public class State06_BroadcastState {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从kafka中获取要处理的数据源,流中的对象具有告警或者恢复的事件
        DataStreamSource<AlertEvent> alertData = env.addSource(new FlinkKafkaConsumer011<>("alert",
                new AlertEventSchema(),
                KafkaUtil.getKafkaProp(ParameterToolUtil.createParameterTool(args)))).setParallelism(1);

        //从数据库中获取数据源,流内对象为:告警语句格式信息
        DataStreamSource<Rule> alarmdata = env.addSource(new MySqlSource());
        // MapState 中保存 (RuleName, Rule) ，在描述类中指定 State name
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {
                }));

        // alarmdata 使用 MapStateDescriptor 作为参数广播，得到广播流
        BroadcastStream<Rule> ruleBroadcastStream = alarmdata.broadcast(ruleStateDescriptor);
        /**
         * 如果非广播流是 keyed stream，需要实现 KeyedBroadcastProcessFunction
         * 如果非广播流是 non-keyed stream，需要实现 BroadcastProcessFunction
         */

        BroadcastConnectedStream<AlertEvent, Rule> connect = alertData.connect(ruleBroadcastStream);
        connect.process(new BroadcastProcessFunction<AlertEvent, Rule, Object>() {

            //普通数据流数据处理
            @Override
            public void processElement(AlertEvent value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {

            }


            //广播流数据处理
            @Override
            public void processBroadcastElement(Rule value, Context ctx, Collector<Object> out) throws Exception {
                //获取广播流对象
                BroadcastState<String, Rule> broadcastState = ctx.getBroadcastState(ruleStateDescriptor);

            }
        }).printToErr();


        /*connect.process(new KeyedBroadcastProcessFunction<Object, AlertEvent, Rule, Object>() {
            @Override
            public void processElement(AlertEvent value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {

            }

            @Override
            public void processBroadcastElement(Rule value, Context context, Collector<Object> out) throws Exception {
                ValueStateDescriptor<Rule> stateDescriptor = new ValueStateDescriptor<>("xiahu", Rule.class);
                MapStateDescriptor<Rule, AlertEvent> ruleMapStateDescriptor = new MapStateDescriptor<Rule, AlertEvent>("xiahu", Rule.class, AlertEvent.class);
                //构建StateTtlConfig
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(1))
                        .cleanupFullSnapshot()//清除过去快照(状态)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                //访问 Broadcast state
                BroadcastState<Rule, AlertEvent> broadcastState = context.getBroadcastState(ruleMapStateDescriptor);
                //查询数据元的时间戳
                Long timestamp = context.timestamp();
                //当前水印
                context.currentWatermark();
                //获取当前处理时间
                context.currentProcessingTime();
                //向旁侧输出（side-outputs）发送数据
                context.output(null, null);
            }
        }).print();*/


    }
}
