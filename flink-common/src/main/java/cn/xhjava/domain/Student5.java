package cn.xhjava.domain;


import java.io.Serializable;

/**
 * @author Xiahu
 * @create 2020/10/27
 */

public class Student5 implements Serializable {
    private String id;
    private String classs;
    private String city;
    private long currentTime;

    public Student5() {
    }

    public Student5(String id, String classs, String city) {
        this.id = id;
        this.classs = classs;
        this.city = city;
    }


    public Student5(String id, String classs, String city, long currentTime) {
        this.id = id;
        this.classs = classs;
        this.city = city;
        this.currentTime = currentTime;
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


    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }

    @Override
    public String toString() {
        return "Student5{" +
                "id='" + id + '\'' +
                ", classs='" + classs + '\'' +
                ", city='" + city + '\'' +
                ", currentTime=" + currentTime +
                '}';
    }
}
