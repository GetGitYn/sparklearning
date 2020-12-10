package com.esen

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


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
 *
 *
 */
object DataFrameLearn {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.types._
    val byteType: ByteType.type = ByteType
    val conf: SparkConf = new SparkConf().setAppName("dataframelearn").setMaster("local[3]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()


  }
}
