package cn.xhjava.flink.stream.pojo;


import java.io.Serializable;

/**
 * @author Xiahu
 * @create 2020/10/27
 */

public class Student4 implements Serializable {
    private String index;
    private String id;
    private String classs;
    private String city;

    public Student4() {
    }

    public Student4(String id, String classs, String city) {
        this.id = id;
        this.classs = classs;
        this.city = city;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClasss() {
        return classs;
    }

    public void setClasss(String classs) {
        this.classs = classs;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "Student4{" +
                "id='" + id + '\'' +
                ", classs='" + classs + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
