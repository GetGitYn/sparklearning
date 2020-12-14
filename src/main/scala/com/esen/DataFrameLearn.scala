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
 *createTempView ：临时视图
 * createGlobalTempView：全局视图，可在多个sparksession 之间共享
 *
 * 处理列/表达式：select 重载   参数：str/column对象
 * select("列名1","列名2")
 * 处理字符串表达式：selectExpr
 * 前两者支持再df 上进行sql
 * org.apache.spark.sql.functions 函数方法
 * expr:把表达式转化成代表的列的操作，返回jizhcolum
 *selectExpr == select(expr()) : select的变种 ，接收一组sql 表达式，sql 表达式可以使用系统预定义的聚合函数
 * 不包含任何聚合操作的有效sql
 * lit: 创建一个列使用字面量 ，返回值column
 *withColumn:添加或者替换同名列的值  允许使用保留字创建列名
 * withColumnRename:重命名列
 * 保留字与关键字：
 * 保留字：列名中包含空格或者连字符等保留字符  使用反引号（`）
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
      StructField("field2", IntegerType),
      StructField("field3", LongType)))

    val row: Row = Row("aa", 1, 2L)
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
    frame.printSchema()
    // 错误会出错，没有分组函数,
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

    val frame2: DataFrame = frame.withColumn("number_One", lit(1))
      frame2.selectExpr("`number_One`").show(2)
      frame2.select(expr("number_One")).show(2)
      frame2.select(col("number_One")).show(2)


  }
}
