package cn.xhjava.flink_07_sideoutput;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.MetricEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class SideoutPut03_SideoutPut {

    private static final OutputTag<MetricEvent> app = new OutputTag<MetricEvent>("app") {
    };
    private static final OutputTag<MetricEvent> web = new OutputTag<MetricEvent>("web") {
    };
    private static final OutputTag<MetricEvent> phone = new OutputTag<MetricEvent>("phone") {
    };


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         */
        DataStreamSource<MetricEvent> streamSource = env.fromElements(DataSource.MetricEvent);

        SingleOutputStreamOperator<MetricEvent> streamOperator = streamSource.process(new ProcessFunction<MetricEvent, MetricEvent>() {
            @Override
            public void processElement(MetricEvent value, Context ctx, Collector<MetricEvent> out) throws Exception {
                String type = value.getType();
                switch (type) {
                    case "app":
                        ctx.output(app, value);
                        break;
                    case "web":
                        ctx.output(web, value);
                        break;
                    case "phone":
                        ctx.output(phone, value);
                        break;
                    default:
                        break;
                }
            }
        });

        streamOperator.getSideOutput(web).printToErr("WEB");
        streamOperator.getSideOutput(app).printToErr("APP");
        streamOperator.getSideOutput(phone).printToErr("PHONE");

        env.execute("");
    }
}
