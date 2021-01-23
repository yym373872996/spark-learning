package person.rulo.spark.learning.connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import person.rulo.spark.learning.common.util.PropertiesUtils;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author rulo
 * @Date 2021/1/3 18:49
 */
public class HiveToKafkaExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.sparkContext().conf().registerKryoClasses((Class<?>[]) Arrays.asList(KafkaProducer.class).toArray());
        spark.table("test.user_device_gender").registerTempTable("user_device_gender");
        String sql = "SELECT device_id FROM user_device_gender where date_num = '20210103'";
        String topic = "test";
        /* rdd handle */
        JavaRDD<Row> javaRDD = spark.sql(sql).rdd().toJavaRDD();
        javaRDD.foreachPartition(rowIterator -> {
            /* kafka producer */
            Properties props = null;
            try {
                props = PropertiesUtils.getProperties("producer.properties");
            } catch (Exception e) {
                e.printStackTrace();
            }
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            /* rdd.foreach */
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String key = row.getString(0);
                String value = row.getString(0);
                // 调用 send() 发送消息后，再通过 get() 方法等待返回结果
                RecordMetadata recordMetadata = null;
                try {
                    recordMetadata = producer.send(new ProducerRecord<String, String>(topic, key, value)).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                System.out.println("发送消息到队列" + topic + ": key=" + key + ", value=" + value);
                if (recordMetadata != null) {
                    System.out.println("消息发送成功，当前offset: " + recordMetadata.offset());
                }
            }
            // 关闭生产者对象
            producer.close();
        });

}
}
