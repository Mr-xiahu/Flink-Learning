package cn.xhjava.domain;

/**
 * @author Xiahu
 * @create 2020/11/2
 * <p>
 * 指标
 */
public class MetricEvent {
    private Integer id;
    private String name;
    private String type;
    private String tags;

    public MetricEvent(Integer id, String name, String type, String tags) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.tags = tags;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "MetricEvent{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", tags='" + tags + '\'' +
                '}';
    }
}
