package cn.xhjava.flink_02_api;

import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.Student;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2020/10/27
 * <p>
 */
public class Operator12_Connect {
    public static void main(String[] args) throws Exception {
        //1.实例化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取输入流
        DataStreamSource<Student> studentDataStreamSource = env.fromElements(DataSource.Studens);
        DataStreamSource<Student> studentDataStreamSource2 = env.fromElements(DataSource.Studens2);
        DataStreamSource<Tuple4<Integer, String, String, Integer>> tuple4DataStreamSource = env.fromElements(DataSource.Tuple4_Student);


        studentDataStreamSource.connect(studentDataStreamSource2);


        DataStream<Student> broadcast = studentDataStreamSource.broadcast();

        //3.执行
        env.execute("flink operator Union");
    }
}
