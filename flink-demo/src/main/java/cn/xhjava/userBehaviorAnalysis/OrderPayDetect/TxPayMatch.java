package cn.xhjava.userBehaviorAnalysis.OrderPayDetect;

import cn.xhjava.userBehaviorAnalysis.OrderPayDetect.functions.TxPayMatchDetect;
import cn.xhjava.userBehaviorAnalysis.domain.OrderEvent;
import cn.xhjava.userBehaviorAnalysis.domain.ReceiptEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.OutputTag;

import java.net.URL;


/**
 * 订单支付表数据与到账表数据关联
 */
public class TxPayMatch {
    // 定义侧输出流标签
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays") {
    };
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        // 读取订单支付事件数据
        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<OrderEvent>(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }))
                // 交易id不为空，必须是pay事件
                .filter(data -> !"".equals(data.getTxId()));

        // 读取到账事件数据
        URL receiptResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(receiptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<ReceiptEvent>(new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        // 将两条流进行连接合并，进行匹配处理，不匹配的事件输出到侧输出流
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect(unmatchedPays,unmatchedReceipts));

        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

        env.execute("tx match detect job");
    }

}
