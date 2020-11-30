package cn.xhjava.flink_11_join;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.OrderEvent;
import cn.xhjava.domain.PayEvent;
import cn.xhjava.util.FlinkEnvConfig;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * @author Xiahu
 * @create 2020/11/27
 * 使用join算子实现双流join
 */
public class Flink_01_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //加载配置
        final ParameterTool parameterTool = ParameterToolUtil.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        FlinkEnvConfig.buildEnv(parameterTool, env);

        //获取订单表datastream
        SingleOutputStreamOperator<OrderEvent> oderEventDataStream = env.fromElements(DataSource.orderEvent)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<OrderEvent>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(OrderEvent lastElement, long extractedTimestamp) {
                        return null;
                    }

                    @Override
                    public long extractTimestamp(OrderEvent element, long previousElementTimestamp) {
                        return element.getEventTime();
                    }
                });

        //获取支付表datastream
        SingleOutputStreamOperator<PayEvent> payEventDataStream = env.fromElements(DataSource.payEvent)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<PayEvent>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(PayEvent lastElement, long extractedTimestamp) {
                        return null;
                    }

                    @Override
                    public long extractTimestamp(PayEvent element, long previousElementTimestamp) {
                        return element.getPryTime();
                    }
                });

        //1.使用join算子实现inner join
        oderEventDataStream
                .join(payEventDataStream)
                .where(new KeySelector<OrderEvent, Object>() {
                    @Override
                    public Object getKey(OrderEvent value) throws Exception {
                        return value.getOrderId();
                    }
                })
                .equalTo(new KeySelector<PayEvent, Object>() {
                    @Override
                    public Object getKey(PayEvent value) throws Exception {
                        return value.getOrderId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<OrderEvent, PayEvent, String>() {
                    @Override
                    public String join(OrderEvent first, PayEvent second) throws Exception {

                        return StringUtils.join(Arrays.asList(first.getOrderId(), second.getOrderId(), first.getOrderName(), second.getPryName()));
                    }
                })
                .printToErr();
        env.execute("");
    }
}
