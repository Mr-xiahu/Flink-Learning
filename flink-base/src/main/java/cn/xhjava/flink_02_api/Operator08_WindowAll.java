package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.Student;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 * WindowAll 将元素按照某种特性聚集在一起，
 * 该函数不支持并行操作，默认的并行度就是 1，所以如果使用这个算子的话需要注意一下性能问题
 */
public class Operator08_WindowAll {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<Student> studentDataStreamSource = env.fromElements(DataSource.Studens);
        DataStreamSource<Tuple4<Integer, String, String, Integer>> tuple4DataStreamSource = env.fromElements(DataSource.Tuple4_Student);

        tuple4DataStreamSource.keyBy(2).windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).sum(3).printToErr();


        //3.执行
        env.execute("flink operator WindowAll");
    }
}
