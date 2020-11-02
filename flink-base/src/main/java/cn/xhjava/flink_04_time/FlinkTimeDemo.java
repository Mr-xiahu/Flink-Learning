package cn.xhjava.flink_04_time;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class FlinkTimeDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //事件时间:事件自己携带的时间,比如说,现在有一个Person类,类中有一个nowTome属性,将该类
        //转成json,此时,这个json就携带了事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //处理时间:事件被处理时的系统时间.比如说:现在从kafka消费一条消息,把这条消息写入hdfs,
        //写入hdfs的这个时间点就是该事件的处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //事件进入Flink的事件(翻译为:摄入时间):指的是该事件进入Flink程序的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    }
}
