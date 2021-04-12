package cn.xhjava.domain;


/**
 * @author Xiahu
 * @create 2020/10/27
 */

public class Student3 {
    private String id;
    private String name;
    private String sex;

    public Student3() {
    }

    public Student3(String id, String name, String sex) {
        this.id = id;
        this.name = name;
        this.sex = sex;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }
}
