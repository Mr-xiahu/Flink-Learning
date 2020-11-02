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
 * 10 秒的时间窗口聚合
 */
public class Operator07_Window {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<Student> studentDataStreamSource = env.fromElements(DataSource.Studens);
        DataStreamSource<Tuple4<Integer, String, String, Integer>> tuple4DataStreamSource = env.fromElements(DataSource.Tuple4_Student);

        tuple4DataStreamSource.keyBy(2).window(TumblingEventTimeWindows.of(Time.seconds(10))).sum(3).printToErr();


        //3.执行
        env.execute("flink operator Window");
    }
}
