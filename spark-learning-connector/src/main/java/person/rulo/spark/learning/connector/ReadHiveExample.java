package person.rulo.spark.learning.connector;

import org.apache.spark.sql.SparkSession;

/**
 * @Author rulo
 * @Date 2021/1/3 22:44
 */
public class ReadHiveExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        /* 执行 spark sql 直接查询 hive 表并存成临时表 */
        spark.sql("SELECT * FROM test.user_device_gender").registerTempTable("t1");
        spark.sql("SELECT * FROM t1").show();
        /* 通过 dbName.tableName 的方式指定 hive 表存成临时表 */
        spark.table("test.user_device_gender").registerTempTable("t2");
        spark.sql("SELECT * FROM t2").show();
    }
}
