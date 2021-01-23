package person.rulo.spark.learning.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import person.rulo.spark.learning.common.initializer.SparkSessionInitializer;
import person.rulo.spark.learning.common.pojo.Person;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Author rulo
 * @Date 2020/8/9 23:07
 */
public class DatsetExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSessionInitializer.getInstance("sparkSQLExample");
//        runBasicDataFrame(sparkSession);
//        runDatasetCreationExample(sparkSession);
//        runInferSchemaExample(sparkSession);
        runProgrammaticSchemaExample(sparkSession);
    }

    /**
     * DataFrame 的基本用法
     * @param sparkSession
     * @throws AnalysisException
     */
    private static void runBasicDataFrame(SparkSession sparkSession) throws AnalysisException {
        // 从 json 文件中读取数据到 DataFrame
        // 使用 option 设置支持 json 数据换行
        Dataset<Row> df = sparkSession.read()
                .option("multiLine", true).option("mode", "PERMISSIVE")
                .json("src/main/resources/people.json");
        // 打印 DataFrame 中全部数据
        df.show();
        // 打印 DataFrame 的 schema
        df.printSchema();
        // 选取特定字段的数据
        df.select("name").show();
        // 选取两个字段，并对所有 age 的 value + 1
        df.select(df.col("name"), df.col("age").plus(1)).show();
        // 选择 age 大于 21 的行
        df.filter(df.col("age").gt(21)).show();
        // 根据年龄聚合并统计条数
        df.groupBy("age").count().show();
        // 将 DataFrame 创建为一个临时视图
        df.createOrReplaceTempView("people");
        // 创建后就可以用 sparkSQL 查询数据了
        Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM people");
        sqlDF.show();
        // 创建为全局临时视图
        df.createGlobalTempView("people");
        // 全局临时视图保存在系统维护的数据库 `global_temp` 下
        sparkSession.sql("SELECT * FROM global_temp.people").show();
        // 全局视图可以跨会话访问
        sparkSession.newSession().sql("SELECT * FROM global_temp.people").show();
    }

    /**
     * 创建 Dataset 的几种方法
     * @param sparkSession
     */
    private static void runDatasetCreationExample(SparkSession sparkSession) {
        // 创建一个 Java bean
        Person person = new Person("Andy", 32);
        // 使用 Encoder 读取 Java 对象到 Dataset
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = sparkSession.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();
        // Encoder 内部对大部分 Java 基本数据类型提供了支持
        // 使用 Encoder 将 Integer 数组创建为 Dataset
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = sparkSession.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        // 对 Dataset 执行 map 转换操作
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder
        );
        System.out.println(Arrays.toString((Integer[])transformedDS.collect()));
        // 使用 Encoder 读取 json 文件到 Dataset
        String path = "src/main/resources/people.json";
        Dataset<Row> peopleDF = sparkSession.read()
                .option("multiLine", true).option("mode", "PERMISSIVE")
                .json(path);
        // 提供相应类型的 Encoder 可以完成 DataFrame 到 Dataset 的转换
        Dataset<Person> peopleDS = peopleDF.as(personEncoder);
        peopleDS.show();
    }

    /**
     * 使用反射推断一组数据的 schema
     * @param sparkSession
     */
    private static void runInferSchemaExample(SparkSession sparkSession) {
        // 通过读取文本文件来创建一个 Person 类型的 RDD
        JavaRDD<Person> peopleRDD = sparkSession.read()
                .textFile("src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        // 为 RDD 指定 schema 类型并创建一个 DataFrame
        Dataset<Row> peopleDF = sparkSession.createDataFrame(peopleRDD, Person.class);
        // 为 DataFrame 创建临时视图
        peopleDF.createOrReplaceTempView("people");
        // 使用 sparkSQL 查询
        Dataset<Row> teenagersDF = sparkSession.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        teenagersDF.show();
        // 通过 Encoder 和 map 的结合使用也可以实现 DataFrame 到 Dataset 的转换
        Encoder<String> stringEncoder = Encoders.STRING();
        // 1. 使用 row.getString(index) 获取 DataFrame 特定下标的数据
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        // 2. 使用 row.getAs(filedName) 获取 DataFrame 特定字段名的数据
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getAs("name"),
                stringEncoder
        );
        teenagerNamesByFieldDF.show();
    }

    /**
     * 以编程的方式指定 schema
     * @param sparkSession
     */
    private static void runProgrammaticSchemaExample(SparkSession sparkSession) {
        // 按行读取文本文件中的数据并存为 String 类型的 RDD
        JavaRDD<String> peopleRDD = sparkSession.sparkContext()
                .textFile("src/main/resources/people.txt", 1)
                .toJavaRDD();
        // 以字符串的方式定义每个字段的名称
        String schemaString = "name age";
        // 定义一个 List<StructField> 存放每个字段的元数据信息
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            // 每个字段的类型都采用 Nullable(String)
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        // 生成 schema
        StructType schema = DataTypes.createStructType(fields);
        // 将 String 类型的 RDD 转换为 Row 类型
        JavaRDD<Row> rowRDD = peopleRDD.map(
                (Function<String, Row>) record -> {
                    String[] attributes = record.split(",");
                    return RowFactory.create(attributes[0], attributes[1].trim());
                }
        );
        // 为 RDD 指定 schema 类型并创建一个 DataFrame
        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowRDD, schema);
        // 创建临时视图
        peopleDataFrame.createOrReplaceTempView("people");
        // 执行 sparkSQL
        Dataset<Row> results = sparkSession.sql("SELECT * FROM people");
        results.show();
        // 将 DataFrame 转换为 Dataset
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "name: " + row.getString(0),
                Encoders.STRING()
        );
        namesDS.show();
    }

}
