package person.rulo.spark.learning.examples;

import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * @Author rulo
 * @Date 2020/8/8 23:46
 */
public class WordCounts {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordCounts").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("E:\\testData\\spark-learning\\examples\\wordCounts\\in\\words.txt").cache();
        lines.map((Function<String, Object>) s -> s);
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> wordOnes = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> wordCounts = wordOnes.reduceByKey((Function2<Integer, Integer, Integer>) (value, toValue) -> value + toValue);

        wordCounts.saveAsTextFile("E:\\testData\\spark-learning\\examples\\wordCounts\\out");
    }
}
