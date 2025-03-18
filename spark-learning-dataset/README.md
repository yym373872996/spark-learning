什么是 DataFrame，什么是 Dataset？

Spark 提供了基于 RDD 之上的一种分布式数据集合叫 Dataset。Dataset 包含了 RDD 的一系列操作，同时还支持以 Spark SQL 作为查询引擎。

DataFrame 是通过列来描述的 Dataset，即 Dataset 下数据的类型都被统一成了列的格式，类似于用数据库表的方式来表示数据对象。在 Java API 中 DataFrame 的表现形式就是 `Dataset<Row>` 类型，而原本的 Dataset 可能是 `Dataset<Integer>` 或 `Dataset<Person>`。因此从 Java 编程的角度可以把 DataFrame 理解为 Dataset 在特殊泛型参数下的表现形式。Dataset 和 DataFrame 之间可以进行转换。