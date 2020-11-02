package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * 对每个元素都进行判断，返回为 true 的元素，如果为 false 则丢弃数据
 */
public class Operator03_Filter {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<String> stringDataStream = env.fromElements(DataSource.MAP);
        stringDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split("\\W+")) {
                    out.collect(word);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (value.equals("xiahu")) {
                    return true;
                }
                return false;
            }
        }).printToErr();

        //3.执行
        env.execute("flink operator Filter");
    }
}
