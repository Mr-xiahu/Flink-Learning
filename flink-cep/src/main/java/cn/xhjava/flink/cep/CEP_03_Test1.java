package cn.xhjava.flink.cep;


import cn.xhjava.flink.cep.domain.OrderEvent;
import cn.xhjava.flink.cep.domain.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author XiaHu
 * @create 2021/11/10
 * <p>
 * 使用Flink Core 实现订单超时支付告警
 *      订单支付超时
 *      在订单创建后15分钟内没有支付,发出告警信息
 */
public class CEP_03_Test1 {
    // 定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        DataStream<OrderEvent> orderEventStream = env.readTextFile("F:\\git\\Flink-Learning\\flink-demo\\src\\main\\resources\\OrderLog.csv")
                //将字符串转为OrderEvent对象
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                //添加watermark
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarksAdapter.Strategy<OrderEvent>(
                                new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(0)) {
                                    @Override
                                    public long extractTimestamp(OrderEvent element) {
                                        return element.getTimestamp() * 1000L;
                                    }
                                }));

        // 自定义处理函数，主流输出正常匹配订单事件，侧输出流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                //根据订单号分组
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect(orderTimeoutTag));

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");
    }
}

/**
 * 在订单的 create 事件到来后注册定时器，15 分钟后触发；
 * 然后再用一个布尔类型的 Value 状态来作为标识位，表明 pay 事件是否发生过。
 * 如果 pay 事件已经发生，状态被置为 true，那么就不再需要做什么操作；
 * 如果 pay 事件一直没来，状态一直为 false，到定时器触发时，就应该输出超时报警信息
 */
class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

    // 定义状态，保存之前点单是否已经来过create、pay的事件
    ValueState<Boolean> isPayedState;
    ValueState<Boolean> isCreatedState;
    // 定义状态，保存定时器时间戳
    ValueState<Long> timerTsState;

    OutputTag<OrderResult> orderTimeoutTag;


    public OrderPayMatchDetect(OutputTag<OrderResult> orderTimeoutTag) {
        this.orderTimeoutTag = orderTimeoutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class));
        isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
    }


    @Override
    public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
        // 1.获取当前状态
        Boolean isPayed = isPayedState.value();
        Boolean isCreated = isCreatedState.value();
        Long timerTs = timerTsState.value();

        // 判断当前事件类型
        if ("create".equals(value.getEventType())) {
            // 1. 如果来的是create，要判断是否支付过
            if (isPayed) {
                // 1.1 如果已经正常支付，输出正常匹配结果
                out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                // 清空状态，删除定时器
                isCreatedState.clear();
                isPayedState.clear();
                timerTsState.clear();
                ctx.timerService().deleteEventTimeTimer(timerTs);
            } else {
                // 1.2 如果没有支付过，注册15分钟后的定时器，开始等待支付事件
                Long ts = (value.getTimestamp() + 15 * 60) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                // 更新状态
                timerTsState.update(ts);
                isCreatedState.update(true);
            }
        } else if ("pay".equals(value.getEventType())) {
            // 2. 如果来的是pay，要判断是否有下单事件来过
            if (isCreated) {
                // 2.1 已经有过下单事件，要继续判断支付的时间戳是否超过15分钟
                if (value.getTimestamp() * 1000L < timerTs) {
                    // 2.1.1 在15分钟内，没有超时，正常匹配输出
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                } else {
                    // 2.1.2 已经超时，输出侧输出流报警
                    ctx.output(orderTimeoutTag, new OrderResult(value.getOrderId(), "payed but already timeout"));
                }
                // 统一清空状态
                isCreatedState.clear();
                isPayedState.clear();
                timerTsState.clear();
                ctx.timerService().deleteEventTimeTimer(timerTs);
            } else {
                // 2.2 没有下单事件，乱序，注册一个定时器，等待下单事件
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L);
                // 更新状态
                timerTsState.update(value.getTimestamp() * 1000L);
                isPayedState.update(true);
            }
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
        // 定时器触发，说明一定有一个事件没来
        if (isPayedState.value()) {
            // 如果pay来了，说明create没来
            ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
        } else {
            // 如果pay没来，支付超时
            ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
        }
        // 清空状态
        isCreatedState.clear();
        isPayedState.clear();
        timerTsState.clear();
    }
}