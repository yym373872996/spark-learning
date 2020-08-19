package person.rulo.spark.learning.common.pojos;

import java.io.Serializable;

/**
 * @Author rulo
 * @Date 2020/8/10 12:15
 */
public class Employee implements Serializable {
    private String name;
    private long salary;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSalary() {
        return salary;
    }

    public void setSalary(long salary) {
        this.salary = salary;
    }
}
