package com.esen


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._


/**
 * import org.apache.spark.sql.types._
 * val b=byte 实例化为特定类型  创建scala 类型的byte
 * import org.apache.spark.sql.types.DataTypes
 * ByteType x = DataTypes.ByteType;
 * 创建java 类型的Byte
 *
 * scala 数据类型及创建方法：
 * ByteType，ShortType ，IntegerType
 * LongType ，FloatType ，DoubleType
 * DecimalType  对应java.math.BigDecimal， StringType，BinaryType Array[Type]
 * BooleanType ，TimestampType ，DateType
 * ArrayType  scala.collection.Seq
 * MapType 对应 scala.collection.Map  MapType（keyType，valueType，[valueContainsNull]）
 *StructType  对应org.apache.spark.sql.Row   StructType（fields） fields是一个包
 * 含多个StructField的Array
 *StructField ： 对应Scala 数据类型    StructField（name，dataType，[nullable]）
 *
 * DataFrame 核心概念
 * schema: 定义dtaframe 列名及类型，可由数据来源定义 schema-on-read  可能导致精度损失,或者显示定义
 * col(),column(): 构造或者引用列 $"列名" ‘列名  表达式的一种
 * 显示引用：df.col("")
 * expression/expr ： expr("(colname-5)*20")对df 的一个或者一组转换操作，对每个记录应用此方法
 * columns :查询df 所有列
 * first:df.first() 查看一行
 * Row: Row() 创建行 val myRow=Row("Hello", null, 1, false)
 * 访问Row:  myRow.getString(0) / myRow(0) 数据为任意类型  转换为String:myRow(0).asInstanceOf[String]
 *
 *Dataframe 转换操作：
 * 添加/删除 列或者行
 * 行转列/列转行
 * 排序
 *
 * 创建DF：
 * session.createDataFrame: 将RDD[A] 类型转换为df A :caseclass tuples Row
 * createDataFrame :生产api 参数：rowRDD: RDD[Row], schema: StructType
 *
 *
 * seq[Row].toDF //对null 的处理一般，生产中不使用
 * createTempView ：临时视图
 * createGlobalTempView：全局视图，可在多个sparksession 之间共享
 *
 * 处理列/表达式：select 重载   参数：str/column对象
 * 查询列：
 * select("列名1","列名2")
 * df("columnname")
 * 处理字符串表达式：selectExpr
 * 前两者支持再df 上进行sql
 * org.apache.spark.sql.functions 函数方法
 * expr:把表达式转化成代表的列的操作，返回jizhcolum
 * selectExpr == select(expr()) : select的变种 ，接收一组sql 表达式，sql 表达式可以使用系统预定义的聚合函数
 * 不包含任何聚合操作的有效sql
 *
 *
 * 添加列：
 * lit: 创建一个列使用字面量 ，返回值column
 * withColumn:添加或者替换同名列的值  允许使用保留字创建列名
 * 修改列：
 * withColumnRename:重命名列
 * drop:删除列 ，参数：string* ,column
 * 更改列类型：withColumn("colmn",Column.cast("other Type"))
 *
 * 行：
 * 过滤行：创建表达式，根据表达式结果过滤结果为false 的行
 * df.where（表达式/列）
 * df.filter（表达式/列）
 *
 *行去重：distinct 是dropDuplicates 的别名
 *追加行：dataframe 不可变 ，
 * 追加行只能通过联合两个dataframe union 操作，两个 dataframe 应该具有相同的模式 联合操作是基于位置而不是基于数据模式schema执行
 *
 * 行排序：
 * sort/order by  参数：列名字符串或者列表达式或者多个列 默认升序
 * df.sort(expr("count asc"))
 * 指定空值在排序列表中的位置
 * null 排在最前面：
 * asc_nulls_first
 * desc_nulss_first
 * null 排在最后面
 * asc_nulls_last
 * desc_nulls_last
 *
 * 处于性能考虑，在进行别的转换之前，先在每个分区内部进行排序
 * sortWithinPartitions: 参数：string* column* 实现分区排序
 * limit:提取前几条数据
 *
 * 重分区： 根据经常过滤的列对数据进行分区 重分区导致shuffle
 * 经常根据某一列进行过滤时：
 * repartition(col("columnname"))
 *repartition :参数：分区数，列
 *
 *
 * 保留字与关键字：
 * 保留字：列名中包含空格或者连字符等保留字符  使用反引号（`）
 *  spark 默认不区分大小写，通过设置spark.sql("set spark.sql.caseSensitiv=true") 设置
 *  sample:随机抽样 按照一定比例从frame 中抽取数据 参数：withreplacement true 有放回，false 无放回
 *
 *
 *
 *
 */
object DataFrameLearn {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.types._
    val byteType: ByteType.type = ByteType
    val conf: SparkConf = new SparkConf().setAppName("dataframelearn").setMaster("local[3]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //加载离线文件数，注册为df
    val frame: DataFrame = session.read.
      format("json").
      load("G:\\Spark-The-Definitive-Guide-master\\Spark-The-Definitive-Guide-master\\data\\flight-data\\json\\2015-summary.json")
    // 将df 转换为表
    frame.createTempView("dfTable");
    //根据一行数据创建df

    val structType: StructType = StructType(Array(
      StructField("field1", StringType),
      StructField("field2", StringType),
      StructField("field3", IntegerType)))

    val row: Row = Row("aa", "bb", 2)
    //方式一：
    val value: RDD[Row] = session.sparkContext.parallelize(Seq(row))
    val frame1: DataFrame = session.createDataFrame(value, structType)

    frame.printSchema()
   // frame1.select("field1","field2").show()

    //frame1.select(frame.col("field1"),col("field2"),expr("field2")).show()

    //frame1.select(expr("field1 as filed11")).show()
    //alis 不改变df 中列名
    //frame1.select(expr("field1 as fi")).alias("field2").show()
    //frame1.selectExpr("field1 as fi","field2 as fu").show()

    //frame1.selectExpr("*","case when field1 == 'aa' then true end as test").show()
    /**
     * DEST_COUNTRY_NAME: string (nullable = true)
     *  ORIGIN_COUNTRY_NAME: string (nullable = true)
     *  count: long (nullable = true)
     */
    //frame.printSchema()
    // 错误会出错，没有分组函数,
    //session.sql("select avg(count),count(distinct(DEST_COUNTRY_NAME)) from dfTable ").show()
    /*frame.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))","(DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME) as country").show()
    //添加列：lit  withcolumn
    frame.select(expr("*"),lit(1)).show()
    frame.select(expr("*"),lit(1) as ("one")).show()
    frame.withColumn("numberOne",lit(1)).show()
   //复制一列，值与count 列相同
    frame.withColumn("count1",col("count")).show()
    //重命名列
    frame.withColumnRenamed("count","count1")*/
    //引用带有保留字或者关键字的列

   /* val frame2: DataFrame = frame.withColumn("number_One", lit(1))
      frame2.selectExpr("`number_One`").show(2)
      frame2.select(expr("number_One")).show(2)
      frame2.select(col("number_One")).show(2)*/

 /*   frame.selectExpr("COUNT").show(2)
     区分大小写
    session.sql("set spark.sql.caseSensitive=true");

    frame.selectExpr("COUNT").show(2)*/

    //删除列
   // frame.drop("DEST_COUNTRY_NAME").show()
    //更改列类型 （强制类型转换） int to long cast column 类方法 ：实现column 的转换  可转换
    // string, boolean, byte, short, int, long, float, double, decimal, date, timestamp
    frame.withColumn("count2",col("count").cast("long")).show(2)

    //行操作：
    //过滤行
    /*frame.where(col("count")>2).show()
    frame.where("count>2").show()
    frame.filter("count<2").show()
    frame.filter(col("count")<2).show()*/

    //去除重复行 如果设置大小写敏感，这个表名会找不到，不知道为啥 ,结果得出来20个1 很奇怪
  /*  session.sql("select  count(distinct(ORIGIN_COUNTRY_NAME,DEST_COUNTRY_NAME)) from dfTable").show()
  //frame.selectExpr("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
    println(frame.dropDuplicates("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").count()+"aa")
    frame.union(frame1).show()
     //行排序
    frame.orderBy("count").show(5)*/
    //不能这样写  不认识
    //frame.sort("count asc","ORIGIN_COUNTRY_NAME desc").show(5)
    frame.orderBy(expr("count desc")).show()
    frame.orderBy(col("count").desc,col("DEST_COUNTRY_NAME").asc).show()
    //df("columnname")=col("columnname")
    //frame.sort(frame("count").asc)
    session.sql("select * from dfTable order by count desc,DEST_COUNTRY_NAME asc").show()

    //指定null 位置
    frame.sort(col("count").asc_nulls_first)
    //分区排序 实现与hive sql 中sortby 排序相同的功能
    frame.sortWithinPartitions("count")
    //重分区
    println(frame.rdd.getNumPartitions)

    //重点：dataframe 如何分区，spark 分区规则 待研究
    //frame.repartition(col("DEST_COUNTRY_NAME"))
    frame.repartition(5,col("DEST_COUNTRY_NAME")).explain()

    frame.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2).explain()

    //驱动器收集数据到本地，当数据量很大的时候会导致驱动器崩溃
    //frame.collect()
    //将每个分区的数据返回给驱动器并一串行 的方式迭代 消耗巨大内存
    //frame.toLocalIterator()



  }
}
