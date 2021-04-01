package cn.xhjava.domain;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class Student2 {
    private Integer id;
    private String name;
    private String sex;



    public Student2() {
    }



    public Student2(Integer id, String name, String sex) {
        this.id = id;
        this.name = name;
        this.sex = sex;
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

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }


}
