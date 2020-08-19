package person.rulo.spark.learning.datasets.udf;

/**
 * @Author rulo
 * @Date 2020/8/10 10:32
 */
public class Average {
    /* 总和 */
    private long sum;
    /* 计数 */
    private long count;

    public Average() {
    }

    public Average(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
