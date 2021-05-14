package cn.xhjava.flink.table.sql.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author Xiahu
 * @create 2021/4/7
 */
public class Split extends TableFunction<Tuple2<String, Integer>> {
    // 实现自定义TableFunction
    // 定义属性，分隔符
    private String separator = ",";

    public Split(String separator) {
        this.separator = separator;
    }

    // 必须实现一个eval方法，没有返回值
    public void eval(String str) {
        for (String s : str.split(separator)) {
            collect(new Tuple2<>(s, s.length()));
        }
    }
}
