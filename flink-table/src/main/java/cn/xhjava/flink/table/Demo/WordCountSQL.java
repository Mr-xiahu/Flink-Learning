package cn.xhjava.flink.table.Demo;


import cn.xhjava.datasource.DataSource;
import cn.xhjava.domain.Word;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.util.ArrayList;


/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        //1.初始化Flink运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.加载配置
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        BatchTableEnvironment btEnv = BatchTableEnvironment.create(env);
        //3.构造DataSet
        DataSet<Word> wordDataSource = env.fromCollection(DataSource.getWordCollection());
        //4.从DataSet中解析Table对象
        DataSet<Word> input = env.fromCollection(DataSource.getWordCollection());

        //DataSet 转sql, 指定字段名
        ////注意domain字段类型
        Table table = btEnv.fromDataSet(input, "word,frequency");
        table.printSchema();

        //注册为一个表
        btEnv.createTemporaryView("WordCount", table);

        Table table02 = btEnv.sqlQuery("select word as word, sum(frequency) as frequency from WordCount GROUP BY word");

        //将表转换DataSet
        DataSet<Word> ds3 = btEnv.toDataSet(table02, Word.class);
        ds3.printToErr();

        env.execute("Word Count By SQL");
    }
}
