package person.rulo.spark.learning.rdd.example

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OperatorsExp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("operatorsExp")
      .getOrCreate()
    val sc = spark.sparkContext

    /** transformation start */

    // map
    // 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4)
//    )
//    val mapRDD = rdd.map(_*2)
//    mapRDD.collect().foreach(println)

    // mapPartitions
    // 以分区为单位对数据进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4), 2
//    )
//    val mapRDD = rdd.mapPartitions(data => data.map(_*2))
//    mapRDD.collect().foreach(println)

    // mapPartitionsWithIndex
    // 类似于mapPartitions，比mapPartitions多一个参数来表示分区号
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4), 2
//    )
//    rdd.partitions.foreach(partition => {
//      println(partition.index)
//    })
//    val mapRDD = rdd.mapPartitionsWithIndex((index, data) =>{
//      index match {
//        case 1 => data.map(_ * 2)
//        case _ => data
//      }
//    })
//    mapRDD.collect().foreach(println)

    // flatMap
    // 将处理的数据进行扁平化后再进行映射处理，所以算子也称为扁平映射。返回一个可迭代的集合
//    val rdd = sc.makeRDD(
//      List(List(1, 2), List(3, 4))
//    )
//    val fmRDD = rdd.flatMap(
//      list => {
//        list
//      }
//    )
//    fmRDD.collect().foreach(println)

    // glom
    // 将RDD中每一个分区变成一个数组，数组中元素类型与原分区中元素类型一致
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4), 2
//    )
//    val gRDD = rdd.glom()
//    gRDD.collect().foreach(data => println(data.mkString(",")))

    // groupBy
    // 根据指定的规则进行分组，分区默认不变，数据会被打乱（shuffle）。极限情况下，数据可能会被分到同一个分区中
    // 一个分区可以有多个组，一个组只能在一个分区中
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4), 2
//    )
//    // groupBy 会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组，相同的key值的数据会被放置在一个组中
//    def groupFunction(num:Int):Int = {
//      num % 2
//    }
//    val groupRDD = rdd.groupBy(groupFunction)
//    groupRDD.collect().foreach(println)

    // filter
    // 根据指定的规则进行筛选过滤，符合规则的数据保留，不符合的丢弃
    // 当数据进行筛选过滤后，分区不变，但是分区内数据可能不均衡，导致数据倾斜
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4), 2
//    )
//    val filterRDD = rdd.filter(_ % 2 == 1)
//    filterRDD.collect().foreach(println)

    // sample
    // 根据指定规则从数据集中采样数据。通过它可以找到数据倾斜的key
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 3, 3, 4), 2
//    )
//    println(rdd.sample(
//      withReplacement = true,
//      2
//      //      1
//    ).collect().mkString(","))

    // distinct
    // 将数据集中的数据去重。使用分布式处理方式实现，与内存集合使用HashSet去重方式不同
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4, 2, 3, 1), 2
//    )
//    val distinctRDD = rdd.distinct()
//    distinctRDD.collect().foreach(println)
//    // 内存集合distinct的去重方式使用 HashSet 去重
//    // List(1,1,2,2).distinct

    // coalesce
    // 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
    // 当Spark程序中存在过多的小任务时，可以通过coalesce合并分区，减少分区个数，进而减少任务调度成本
//    val rdd = sc.makeRDD(
//      List(1, 2, 3, 4, 5, 6), 3
//    )
//    // 无shuffle
//    val coaRDD = rdd.coalesce(2)
//    // 有shuffle
//    // val coaRDD = rdd.coalesce(2, true)
//    coaRDD.saveAsTextFile("output1")

    // sortBy
    // 根据指定规则进行排序，默认升序，设置第二个参数改变排序方式
    // 默认情况下，不会改变分区个数，但是中间存在shuffle处理
//    val rdd = sc.makeRDD(
//      List(4, 5, 1, 3, 2, 6), 2
//    )
//    val sortRDD = rdd.sortBy(num => num)
//    sortRDD.saveAsTextFile("output")

    // intersection
    // 两个RDD求交集
//    val rdd1 = sc.makeRDD(
//      List(1, 2, 3, 4, 5, 6), 2
//    )
//    val rdd2 = sc.makeRDD(
//      List(3, 4, 5, 6, 7, 8), 2
//    )
//    val rdd = rdd1.intersection(rdd2)
//    rdd.collect().foreach(println)

    // union
    // 两个RDD求并集
//    val rdd1 = sc.makeRDD(
//      List(1, 2, 3, 4, 5, 6), 2
//    )
//    val rdd2 = sc.makeRDD(
//      List(3, 4, 5, 6, 7, 8), 2
//    )
//    val rdd = rdd1.union(rdd2)
//    rdd.collect().foreach(println)

    // subtract
    // 两个RDD求差集
//    val rdd1 = sc.makeRDD(
//      List(1, 2, 3, 4, 5, 6), 2
//    )
//    val rdd2 = sc.makeRDD(
//      List(3, 4, 5, 6, 7, 8), 2
//    )
//    val rdd = rdd1.subtract(rdd2)
//    rdd.collect().foreach(println)

    // zip
    // 拉链操作，以键值对的形式进行合并
//    val rdd1 = sc.makeRDD(List(1,2,3,4))
//    val rdd2 = sc.makeRDD(List(3,4,5,6))
//    // 要求两个数据源数据类型保持一致
//    // 拉链, 对应位置一对一映射，组成（key,value），需要每个对应分区上的数据个数相同
//    val newRDD = rdd1.zip(rdd2)
//    println(newRDD.collect().mkString(","))

    // partitionBy
    // 将数据按照指定partitioner重新进行分区，默认的分区器是HashPartitioner
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    val newRDD:RDD[(Int, Int)] = rdd.map((_, 1))
//    // partitionBy 根据指定的分区规则对数据进行重分区
//    newRDD.partitionBy(new HashPartitioner(2))
//      .saveAsTextFile("output")

    // reduceByKey
    // 将数据按照相同的key对value进行聚合
//    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",2)))
//    // reduceByKey 相同的Key的数据进行value数据的聚合操作
//    // scala 语言中一般的聚合都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合的
//    // reduceByKey 中如果key的数据只有一个，是不会参与运算的
//    val reduceRDD = rdd.reduceByKey(_ + _)
//    reduceRDD.collect().foreach(println)

    // groupByKey
    // 将数据按照相同的key对value进行分组，形成一个对偶元祖
//    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",2)))
//    // groupByKey : 将数据源中的数据，相同的key的数据分到一个组中，形成一个对偶元组
//    // 元组中的第一个元素就是key，第二个元素就是相同key的value集合
//    val groupRDD = rdd.groupByKey()
//    groupRDD.collect().foreach(println)

    // aggregateByKey
//    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)
//    // 分区内和分区间的计算规则可以不同，也可以相同
//    rdd.aggregateByKey(3)( // 每个分区内的值和初始值(zeroValue)进行一次分区内计算，分区间计算初始值不参与
//      (x,y) => math.max(x,y),
//      (x,y) => x + y
//    ).collect().foreach(println)

    // foldByKey
    // aggregateByKey的简化操作，分区内和分区间的计算规则一样
//    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)
//    // 分区内和分区间的计算规则可以相同
//    //   rdd.aggregateByKey(0)(
//    //     (x,y) => x + y,
//    //     (x,y) => x + y
//    //   ).collect().foreach(println)
//    // 可以使用foldByKey来简化
//    rdd.foldByKey(0)(_+_).collect().foreach(println)

    // combineByKey
    // 针对相同K，将V合并成一个集合
//    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),
//      ("b",4),("b",5),("a",6)),2)
//    // 获取相同key的数据的平均值 => (a,3) (b,4)
//    // combineByKey 需要三个参数
//    // 第一个参数表示：将相同key的第一个数据进行数据结构的转换，实现操作
//    // 第二个参数表示：分区内的计算规则
//    // 第三个参数表示：分区间的计算规则
//    val newRDD = rdd.combineByKey(
//      v => (v, 1), // 转换为 tuple是在运行当中动态得到的，所以下面的tuple需要添加数据类型
//      (t:(Int, Int), v) => {
//        (t._1 + v, t._2 + 1)
//      },
//      (t1:(Int, Int), t2:(Int, Int)) => {
//        (t1._1 + t2._1, t1._2 + t2._2)
//      }
//    )
//    val resultRDD = newRDD.mapValues {
//      case (sum, cnt) => sum / cnt
//    }
//    resultRDD.foreach(println)

    // join
    // 在类型为（K，V）和（K，W）的RDD上调用，返回一个相同的key对应的所有元素连接在一起的（K，（V，W））的RDD
//    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
//    val rdd2 = sc.makeRDD(List(("a",5),("a",6),("e",8),("c",7)))
//    // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组。
//    //  如果两个数据源中key没有匹配上，那么数据不会出现在结果中。
//    //  如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔积，数据量会几何性增长，会导致性能降低
//    rdd.join(rdd2).collect().foreach(println)

    // sortByKey
    // 在一个(K,V)的RDD上调用，K必须实现ordered接口，返回一个按照key进行排序的(K,V)的RDD
//    val rdd = sc.makeRDD(List(("a",3),("b",2),("c",1),("d",4)))
//    //按照key对rdd中的元素进行排序，默认升序
//    rdd.sortByKey().collect().foreach(println)
//    //降序
//    rdd.sortByKey(ascending = false).collect().foreach(println)

    // mapValues
    // 针对于(K,V)形式的类型只对V进行操作
//    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
//    rdd.mapValues("pre_"+_).collect().foreach(println)

    // cogroup
    // 相同的key，value分组后连接起来
//    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
//    val rdd2 = sc.makeRDD(List(("a",5),("a",6),("e",8),("c",7)))
//    // cogroup ： connect + group (分组，连接)
//    // 可以有多个参数
//    rdd.cogroup(rdd2).collect().foreach(println)

    /** transformation end */

    /** action start */

    // reduce
    // 聚合RDD中的所有数据，先聚合分区内数据，在聚合分区间数据
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    val i = rdd.reduce(_ + _)
//    println(i)

    // collect
    // 采集，该方法会将不同分区间的数据按照分区顺序采集到Driver端，形成数组
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    val arr = rdd.collect()
//    arr.foreach(println)

    // count
    // 统计数据个数
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    val l = rdd.count()
//    println(l)

    // first
    // 获取RDD中的第一个元素
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    val f = rdd.first()
//    println(f)

    // take
    // 获取RDD前n个元素组成的数组
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    val arr = rdd.take(2)
//    arr.foreach(println)

    // takeOrdered
    // 获取RDD排序后的前n个元素组成的数组
//    val rdd = sc.makeRDD(List(1,3,2,4),2)
//    // 数据先排序，再取N个，默认升序排序，可以使用第二个参数列表（比如：Ordering.Int.reverse）实现倒序功能
//    val arr = rdd.takeOrdered(2)(Ordering.Int.reverse)
//    arr.foreach(println)

    // aggregate
    // 将每个分区里面的元素通过分区内逻辑和初始值(zeroValue)进行聚合，然后用分区间逻辑再次和初始值进行操作
    // 注意：分区间逻辑会再次使用初始值，这和aggregateByKey只在分区内使用是有区别的
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 5)
//    println(rdd.aggregate(10)(_ + _, _ + _))

    // fold
    // 折叠操作，aggregate的简化操作，分区内逻辑和分区间逻辑相同
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 5)
//    println(rdd.fold(10)(_ + _))

    // countByValue
    // 统计每个value的个数
//    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
//    println(rdd.countByValue())

    // countByKey
    // 统计每种key的个数
//    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
//    println(rdd.countByKey())

    // foreach
    // 遍历RDD中每一个元素
//    val rdd = sc.makeRDD(List(1,2,3,4),2)
//    rdd.foreach(println)

    // save
    // （1）saveAsTextFile(path)保存成Text文件
    // （2）saveAsSequenceFile(path) 保存成SequenceFile文件
    // （3）saveAsObjectFile(path) 序列化成对象保存到文件

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    // saveAsSequenceFile 方法要求数据的格式必须为 K-V 键值对类型
    rdd.saveAsSequenceFile("output2")

    /** action end */

    sc.stop()

  }

}
