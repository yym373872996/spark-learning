package person.rulo.spark.learning.common.initializers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Author rulo
 * @Date 2020/8/9 0:14
 */
public class SparkContextInitializer {
    private static JavaSparkContext sparkContext;

    private SparkContextInitializer() {
    }

    public static JavaSparkContext getInstance(String appName) {
        if (sparkContext == null) {
            synchronized (SparkContextInitializer.class) {
                if (sparkContext == null) {
                    SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local");
                    sparkContext = new JavaSparkContext(sparkConf);
                }
            }
        }
        return sparkContext;
    }
}
