package person.rulo.spark.learning.common.initializers;

import org.apache.spark.sql.SparkSession;

/**
 * @Author rulo
 * @Date 2020/8/9 0:14
 */
public class SparkSessionInitializer {
    private static volatile SparkSession sparkSession;

    private SparkSessionInitializer() {
    }

    public static SparkSession getInstance(String appName) {
        if (sparkSession == null) {
            synchronized (SparkSessionInitializer.class) {
                if (sparkSession == null) {
                    sparkSession = SparkSession.builder().appName(appName).master("local").getOrCreate();
                }
            }
        }
        return sparkSession;
    }
}
