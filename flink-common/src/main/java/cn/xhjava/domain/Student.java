package cn.xhjava.domain;

/**
 * @author Xiahu
 * @create 2020/10/27
 */
public class Student {
    private Integer id;
    private String name;
    private String sex;

    private Integer sorce;


    public Student() {
    }

    public Student(Integer id, String name, String sex, Integer sorce) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.sorce = sorce;
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

    public Integer getSorce() {
        return sorce;
    }

    public void setSorce(Integer sorce) {
        this.sorce = sorce;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", sorce=" + sorce +
                '}';
    }
}
