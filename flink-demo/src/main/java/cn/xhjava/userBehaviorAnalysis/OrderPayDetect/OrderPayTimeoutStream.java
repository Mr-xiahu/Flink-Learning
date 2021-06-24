package cn.xhjava.userBehaviorAnalysis.OrderPayDetect;

import cn.xhjava.userBehaviorAnalysis.OrderPayDetect.functions.OrderPayMatchDetect;
import cn.xhjava.userBehaviorAnalysis.domain.OrderEvent;
import cn.xhjava.userBehaviorAnalysis.domain.OrderResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.OutputTag;


/**
 * 订单支付超时
 * 在 pay 事件超时未发生的情况下，输出超时报警信息
 */
public class OrderPayTimeoutStream {
    // 定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        DataStream<OrderEvent> orderEventStream = env.readTextFile("F:\\git\\Flink-Learning\\flink-demo\\src\\main\\resources\\OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<OrderEvent>(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        // 自定义处理函数，主流输出正常匹配订单事件，侧输出流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect(orderTimeoutTag));

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");
    }

}
