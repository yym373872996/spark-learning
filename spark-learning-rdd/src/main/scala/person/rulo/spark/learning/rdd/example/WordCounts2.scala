package person.rulo.spark.learning.rdd.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.regex.Pattern

object WordCounts2 {

  private val SPACE = Pattern.compile(" ")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("wordCounts2")
      .getOrCreate()
    val sparkContext = spark.sparkContext

    val lines: RDD[String] = sparkContext
      .textFile("/Users/rulo/Workspaces/data/word_counts/in/input.txt")
      .cache
    val wordCounts = lines
      .flatMap((s: String) => SPACE.split(s))
      .map(s => Tuple2(s, 1))
      .reduceByKey((v1, v2) => v1 + v2)
    wordCounts.saveAsTextFile("/Users/rulo/Workspaces/data/word_counts/out")
  }

}
