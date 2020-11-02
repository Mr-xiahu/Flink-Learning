package cn.xhjava.domain;

/**
 * @author Xiahu
 * @create 2020/11/2
 */
public class Event {
    private Integer id;
    private String name;
    private long timeStamp;


    public Event(Integer id, String name, long timeStamp) {
        this.id = id;
        this.name = name;
        this.timeStamp = timeStamp;
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

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
