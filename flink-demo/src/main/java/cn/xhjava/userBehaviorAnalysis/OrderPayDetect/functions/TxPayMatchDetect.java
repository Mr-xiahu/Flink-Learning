package cn.xhjava.userBehaviorAnalysis.OrderPayDetect.functions;


import cn.xhjava.userBehaviorAnalysis.domain.OrderEvent;
import cn.xhjava.userBehaviorAnalysis.domain.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author XiaHu
 * @create 2021/5/23
 * 实现自定义CoProcessFunction,到账数据与支付数据关联
 */
public class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
    // 定义状态，保存当前已经到来的订单支付事件和到账时间
    ValueState<OrderEvent> payState;
    ValueState<ReceiptEvent> receiptState;

    OutputTag<OrderEvent> unmatchedPays;
    OutputTag<ReceiptEvent> unmatchedReceipts;

    public TxPayMatchDetect(OutputTag<OrderEvent> unmatchedPays, OutputTag<ReceiptEvent> unmatchedReceipts) {
        this.unmatchedPays = unmatchedPays;
        this.unmatchedReceipts = unmatchedReceipts;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
        receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class));
    }


    @Override
    public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        // 订单支付事件来了，判断是否已经有对应的到账事件
        ReceiptEvent receipt = receiptState.value();
        if (receipt != null) {
            // 如果receipt不为空，说明到账事件已经来过，输出匹配事件，清空状态
            out.collect(new Tuple2<>(pay, receipt));
            payState.clear();
            receiptState.clear();
        } else {
            // 如果receipt没来，注册一个定时器，开始等待
            ctx.timerService().registerEventTimeTimer((pay.getTimestamp() + 5) * 1000L);    // 等待5秒钟，具体要看数据
            // 更新状态
            payState.update(pay);
        }
    }

    @Override
    public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        // 到账事件来了，判断是否已经有对应的支付事件
        OrderEvent pay = payState.value();
        if (pay != null) {
            // 如果pay不为空，说明支付事件已经来过，输出匹配事件，清空状态
            out.collect(new Tuple2<>(pay, receipt));
            payState.clear();
            receiptState.clear();
        } else {
            // 如果pay没来，注册一个定时器，开始等待
            ctx.timerService().registerEventTimeTimer((receipt.getTimestamp() + 3) * 1000L);    // 等待3秒钟，具体要看数据
            // 更新状态
            receiptState.update(receipt);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        // 定时器触发，有可能是有一个事件没来，不匹配，也有可能是都来过了，已经输出并清空状态
        // 判断哪个不为空，那么另一个就没来
        if (payState.value() != null) {
            ctx.output(unmatchedPays, payState.value());
        }
        if (receiptState.value() != null) {
            ctx.output(unmatchedReceipts, receiptState.value());
        }
        // 清空状态
        payState.clear();
        receiptState.clear();
    }
}