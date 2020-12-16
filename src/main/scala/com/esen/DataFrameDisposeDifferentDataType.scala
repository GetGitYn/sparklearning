package com.esen



import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, expr, lit, pow}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


/**
 *  todo
 *  IS [NOT] DISTINCT FROM 2.3 支持 待测
 *
 * DataFrameStatFunctions : 包含统计相关的函数
 * DataFrameNaFunctions ：空值处理办法
 *
 *处理数据类型：布尔，数字，字符串，日期和时间戳 空值 复杂 用户自定义
 *
 * lit：原始数据转换成spark，返回col
 *
 */
object DataFrameDisposeDifferentDataType {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dataframelearndispose").setMaster("local[3]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = session.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("G:\\Spark-The-Definitive-Guide-master\\Spark-The-Definitive-Guide-master\\data\\retail-data\\by-day\\2010-12-01.csv")

     frame.printSchema()
     frame.createOrReplaceTempView("dfTable")


    /**
     * |-- InvoiceNo: string (nullable = true)
     * |-- StockCode: string (nullable = true)
     * |-- Description: string (nullable = true)
     * |-- Quantity: integer (nullable = true)
     * |-- InvoiceDate: timestamp (nullable = true)
     * |-- UnitPrice: double (nullable = true)
     * |-- CustomerID: double (nullable = true)
     * |-- Country: string (nullable = true)
     */
  /*  frame.select(lit(5),lit("five"),lit(5.0)).show()

    /**
     * show():参数：行数，是否省略超过20个字符的字符串，默认true
     */
    session.sql("select 5,\"five\",5.0").show(1)*/


      /**
       * 处理boolean 类型-》过滤操作的基础->过滤
       */

   /* session.sql("select InvoiceNo,Description from dfTable where InvoiceNo=536365").show()
    frame.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo","Description").show(5,false)
  //或
    frame.where(col("InvoiceNo")===536365)
      .select("InvoiceNo","Description").show(5,false)
//或者
    frame.where("InvoiceNo=536365")
      .select("InvoiceNo","Description").show(5,false)*/

  //连接多个boolean 表达式 ->链式连接——》spark 顺序执行

    //这个写法有错误，不支持* 作为通配
    //session.sql("select * from dfTable where StockCode in ('DOT') and (Description like *POSTAGE* or UnitPrice>600)").show()
    //权威指南写法：instr 字符串函数 ，截取字符串位置 类似locate 位置从 1 开始
   // session.sql("SELECT * FROM dfTable WHERE StockCode in (\"DOT\") AND(UnitPrice > 600 OR instr(Description, \"POSTAGE\") >= 1)").show()
    //这中写法，前面的条件被要求一定为true 不对 * 通配不认识
    frame.where(col("StockCode").isin("DOT")).where(col("UnitPrice").gt(600).or(col("Description").contains("POSTAGE")))

    //过滤器不一定非要用boolean 表达式 可采用将表达式的结果存为一个新列，选出结果为true 的列

    //这个写的也不对
   // session.sql("select unitPrice,newLine from( select unitPrice,case when StockCode==\"NOT\" and (UnitPrice>600 or instr(Description,\"POSTAGE\")>=1 then true else false end newLine from dfTable ) a where newLine is true)").show()

    //权威指南写：

    val margin: String =
      """
        |SELECT UnitPrice, (StockCode = 'DOT' AND
        |(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
        |FROM dfTable
        |WHERE (StockCode = 'DOT' AND
        |(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))
        |""".stripMargin
        session.sql(margin).show()

    frame.withColumn("newLine",(col("StockCode").equalTo("DOT").and(col("UnitPrice").gt(600).or(col("Description").contains("POSTAGE")))))
      .where("newLine").select("UnitPrice","newLine").show()

    //处理空值 执行空值的安全等价 IS [NOT] DISTINCT FROM 2.3 支持 待测
    frame.where(col("Description").eqNullSafe("hello")).show()

    /**
     * 处理数值类型:
     * 计数：
     * pow ：幂运算 返回column =>power()
     * round:向上取整
     * bround:想下去整
     *
     */
    val column: Column = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    frame.select(expr("CustomerId"),column.alias("antherString")).show()
    frame.selectExpr("CustomerId","(POWER((Quantity * UnitPrice), 2.0)+5) AS  realQuantity").show(2)

    session.sql("select CustomerId,POWER((Quantity * UnitPrice),2.0)+5 AS newline from dfTable").show()





  }


}
