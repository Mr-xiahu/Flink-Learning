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
public class Operator09_Union {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<Student> studentDataStreamSource = env.fromElements(DataSource.Studens);
        DataStreamSource<Student> studentDataStreamSource2 = env.fromElements(DataSource.Studens2);
        DataStreamSource<Tuple4<Integer, String, String, Integer>> tuple4DataStreamSource = env.fromElements(DataSource.Tuple4_Student);

        studentDataStreamSource
                .union(studentDataStreamSource2)
                .keyBy("sex")
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum("sorce")
                .printToErr();


        //3.执行
        env.execute("flink operator Union");
    }
}
