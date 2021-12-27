package cn.xhjava.flink_12_savepoint;

/**
 * @author Xiahu
 * @create 2021/12/24 0024
 */
public class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {
    }

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
