package cn.xhjava.flink_11_join;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.OrderEvent;
import cn.xhjava.domain.PayEvent;
import cn.xhjava.util.FlinkEnvConfig;
import cn.xhjava.util.ParameterToolUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * @author Xiahu
 * @create 2020/11/27
 * 双流join 将五分钟之内的订单信息和支付信息进行对账，对不上的发出警告
 */
public class MulitDataStreamJoin {
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
        /*oderEventDataStream
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
                .printToErr();*/

        //2.使用coGroup,实现left join
        /*oderEventDataStream
                .coGroup(payEventDataStream)
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
                .apply(new CoGroupFunction<OrderEvent, PayEvent, String>() {
                    @Override
                    public void coGroup(Iterable<OrderEvent> first, Iterable<PayEvent> second, Collector<String> out) throws Exception {
                        for (OrderEvent orderEvent : first) {
                            Boolean isMatched = false;
                            for (PayEvent payEvent : second) {
                                //右流中存在与之关联的数据
                                out.collect(StringUtils.join(Arrays.asList("true", orderEvent.getOrderId(), payEvent.getOrderId(), orderEvent.getOrderName(), payEvent.getPryName())));
                                isMatched = true;
                            }

                            if (!isMatched) {
                                //右流中不存在与之关联的数据
                                out.collect(StringUtils.join(Arrays.asList("false", orderEvent.getOrderId(), orderEvent.getOrderName(), null, null)));
                            }
                        }

                    }
                })
                .printToErr();*/

        //2.使用coGroup,实现right join
        payEventDataStream
                .coGroup(oderEventDataStream)
                .where(new KeySelector<PayEvent, Object>() {
                    @Override
                    public Object getKey(PayEvent value) throws Exception {
                        return value.getOrderId();
                    }
                })
                .equalTo(new KeySelector<OrderEvent, Object>() {
                    @Override
                    public Object getKey(OrderEvent value) throws Exception {
                        return value.getOrderId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<PayEvent, OrderEvent, String>() {
                    @Override
                    public void coGroup(Iterable<PayEvent> first, Iterable<OrderEvent> second, Collector<String> out) throws Exception {
                        for (PayEvent payEvent : first) {
                            Boolean isMatched = false;
                            for (OrderEvent orderEvent : second) {
                                //右流中存在与之关联的数据
                                out.collect(StringUtils.join(Arrays.asList("true", payEvent.getPayId(), orderEvent.getOrderId(), payEvent.getPryName(), orderEvent.getOrderName())));
                                isMatched = true;
                            }

                            if (!isMatched) {
                                //右流中不存在与之关联的数据
                                out.collect(StringUtils.join(Arrays.asList("false", payEvent.getPayId(), payEvent.getPryName(), null, null)));
                            }
                        }

                    }
                })
                .printToErr();
        System.out.println(env.getExecutionPlan());
        env.execute("");
    }
}