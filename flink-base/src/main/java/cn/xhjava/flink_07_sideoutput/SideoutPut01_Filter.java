package cn.xhjava.flink_07_sideoutput;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.MetricEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class SideoutPut01_Filter {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 使用Fliter分流,但是不支持多次分流.比如说,将app这条流再次使用filter分割,会出错
         */
        DataStreamSource<MetricEvent> streamSource = env.fromElements(DataSource.MetricEvent);
        //streamSource.printToErr();

        //过滤出app的数据
        streamSource.filter(new FilterFunction<MetricEvent>() {
            @Override
            public boolean filter(MetricEvent value) throws Exception {
                boolean result = false;
                if ("app".equals(value.getType())) {
                    result = true;
                }
                return result;
            }
        }).printToErr();

        env.execute("");
    }
}
