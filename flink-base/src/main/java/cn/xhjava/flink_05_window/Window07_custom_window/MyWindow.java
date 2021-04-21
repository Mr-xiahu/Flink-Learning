package cn.xhjava.flink_05_window.Window07_custom_window;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class MyWindow<T, KEY> extends KeyedStream<T, KEY> {
    public MyWindow(DataStream<T> dataStream, KeySelector<T, KEY> keySelector) {
        super(dataStream, keySelector);
    }

    public MyWindow(DataStream<T> dataStream, KeySelector<T, KEY> keySelector, TypeInformation<KEY> keyType) {
        super(dataStream, keySelector, keyType);
    }
}
