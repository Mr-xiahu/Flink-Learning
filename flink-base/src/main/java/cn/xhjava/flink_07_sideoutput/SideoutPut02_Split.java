package cn.xhjava.flink_07_sideoutput;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.MetricEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class SideoutPut02_Split {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         *  使用Split分流,但是不支持多次分流.比如说,将app这条流再次使用Split分割,会出错
         */
        DataStreamSource<MetricEvent> streamSource = env.fromElements(DataSource.MetricEvent);
        SplitStream<MetricEvent> splitStream = streamSource.split(new OutputSelector<MetricEvent>() {
            @Override
            public Iterable<String> select(MetricEvent value) {
                List<String> tags = new ArrayList<>();
                String type = value.getType();
                switch (type) {
                    case "app":
                        tags.add("app");
                        break;
                    case "web":
                        tags.add("web");
                        break;
                    case "phone":
                        tags.add("phone");
                        break;
                    default:
                        break;
                }
                return tags;
            }
        });

        splitStream.select("app").print();
        splitStream.select("phone").print();
        splitStream.select("web").printToErr();


        env.execute("");
    }
}
