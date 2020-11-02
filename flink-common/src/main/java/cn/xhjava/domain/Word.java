package cn.xhjava.domain;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class Word {
    public String word;
    public long frequency;

    public Word() {
    }

    public Word(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return word + ',' +
                frequency
                ;
    }
}
