package cn.xhjava.domain;

/**
 * @author Xiahu
 * @create 2020/1/6
 * @since 1.0.0
 */
public class Rule {
    private String id;
    private String content;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Rule(String id, String content) {
        this.id = id;
        this.content = content;
    }
}