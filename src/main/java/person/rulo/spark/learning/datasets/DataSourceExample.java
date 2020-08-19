package person.rulo.spark.learning.datasets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import person.rulo.spark.learning.common.initializers.SparkSessionInitializer;

import java.util.Properties;

/**
 * @Author rulo
 * @Date 2020/8/10 15:40
 */
public class DataSourceExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSessionInitializer.getInstance("dataSourceExample");
//        runBasicDataSourceExample(sparkSession);
        runJdbcDatasetExample(sparkSession);
    }

    /**
     * 连接并操作基本数据源的示例
     * 包括 parquet，json，csv，orc
     * @param sparkSession
     */
    private static void runBasicDataSourceExample(SparkSession sparkSession) {
        // 读取一个 parquet 文件到 DataFrame
        Dataset<Row> usersDF = sparkSession.read().load("src/main/resources/users.parquet");
        // 将 DataFrame 写入到 parquet
        usersDF.select("name", "favorite_color").write().save("output/namesAndFavColors.parquet");
        // 读取 json 文件到 DataFrame 并写入到 parquet
        Dataset<Row> peopleDF = sparkSession.read()
                .format("json")
                .option("multiLine", true).option("mode", "PERMISSIVE")
                .load("src/main/resources/people.json");
        peopleDF.select("name", "age").write().format("parquet").save("output/namesAndAges.parquet");
        // 读取 csv
        Dataset<Row> peopleDFCsv = sparkSession.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/main/resources/people.csv");
        peopleDFCsv.show();
        // 写入 csv
        peopleDF.write().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .save("output/people_with_options.csv");
        // 读取 orc
        Dataset<Row> userDFOrc = sparkSession.read().format("orc")
                .option("orc.bloom.filter.columns", "favorite_color")
                .option("orc.dictionary.key.threshold", "1.0")
                .option("orc.column.encoding.direct", "name")
                .load("src/main/resources/users.orc");
        userDFOrc.show();
        // 写入 orc
        usersDF.write().format("orc")
                .option("orc.bloom.filter.columns", "favorite_color")
                .option("orc.dictionary.key.threshold", "1.0")
                .option("orc.column.encoding.direct", "name")
                .save("output/users_with_options.orc");
        // 直接在文件上运行 sql
        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`");
        sqlDF.show();
        // 对 DataFrame 进行分桶和排序并持久化到 Hive metastore 中
        peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");
        // 对 DataFrame 进行分区并以 parquet 形式存储
        usersDF.write()
                .partitionBy("favorite_color")
                .format("parquet")
                .save("output/namesPartByColor.parquet");
        // 对 DataFrame 进行分区并持久化到 Hive metastore 中
        peopleDF.write()
                .partitionBy("favorite_color")
                .bucketBy(42, "name")
                .saveAsTable("people_partitioned_bucketed");
        // 对持久化表执行 sql
        sparkSession.sql("DROP TABLE IF EXISTS people_bucketed");
        sparkSession.sql("DROP TABLE IF EXISTS people_partitioned_bucketed");
    }

    /**
     * 连接并操作 jdbc 数据源的示例
     * @param sparkSession
     */
    public static void runJdbcDatasetExample(SparkSession sparkSession) {
        // 连接 jdbc 数据库并读取数据到 DataFrame
        // 方式 1
        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/test")
                .option("dbtable", "public.people")
                .option("user", "postgres")
                .option("password", "123456")
                .load();
        jdbcDF.show();
        // 方式 2
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        Dataset<Row> jdbcDF2 = sparkSession.read()
                .jdbc("jdbc:mysql://localhost:3306/test", "people", connectionProperties);
        jdbcDF2.show();
        // 将 DataFrame 写入到 jdbc 数据库
        // 方式 1
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/test")
                .option("dbtable", "public.people")
                .option("user", "postgres")
                .option("password", "123456")
                .mode(SaveMode.Append)
                .save();
        // 方式 2
        jdbcDF2.write()
                .mode(SaveMode.Append)
                .jdbc("jdbc:mysql://localhost:3306/test", "people", connectionProperties);

    }
}
