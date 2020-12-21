package com.esen






import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array_contains, bround, coalesce, col, corr, current_date, current_timestamp, date_add, date_sub, datediff, explode, expr, from_json, initcap, lit, lpad, ltrim, monotonically_increasing_id, pow, regexp_extract, regexp_replace, round, rpad, rtrim, size, split, struct, to_date, to_timestamp, translate, trim, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
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

  def power3(number:Double):Double = number * number * number
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
     *
     */
    val column: Column = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    frame.select(expr("CustomerId"),column.alias("antherString")).show()
    frame.selectExpr("CustomerId","(POWER((Quantity * UnitPrice), 2.0)+5) AS  realQuantity").show(2)

    session.sql("select CustomerId,POWER((Quantity * UnitPrice),2.0)+5 AS newline from dfTable").show()
    frame.select(round(col("UnitPrice"),1).alias("rounded"),col("UnitPrice")).show()
   //
    frame.select(round(lit("2.5")),bround(lit("2.5"))).show(2)
    session.sql("select round(2.5),bround(2.5)")


    //session.sql("select ") 计算两列的相关列 pearson 系数 反应两列的线性相关程度
    //frame.stat 返回 DataFrameStatFunctions  支持统计工作
    frame.stat.corr("Quantity","UnitPrice")

    //corr 计算两列的皮尔森系数，返回列
    /*frame.select(corr("Quantity","UnitPrice")).show()

    session.sql("select corr(Quantity,UnitPrice) from dfTable").show()*/

    /**
     * 计算一组或者一组列的汇总信息  describe
     * describe: 计算所有数值型列的计数，均值，标准差，最大最小值,describe 参数为空时计算schema 中所有的数值列
     * 或者专门的函数计算某列的聚合
     * count：累加
     * mean：均值
     * stddev_pop：标准差
     * min :最大值
     * max：最小值
     *  这  都      的方法               approxQuantile ：每列给定概率下的近似分位数 参数：列名 概率  相对精度目标
     *  三  是                        交叉列表：crosstab
     *  个  DataFrameStatFunctions      频繁项对：freqItems
     * monotonically_increasing_id ： return:column  为每行添加唯一id 从0开始,非连续，id值与分区值有关
     *
     */

    frame.describe().show()

    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    /*frame.stat.approxQuantile("UnitPrice", quantileProbs, relError)

    frame.stat.crosstab("StockCode", "Quantity").show()

    frame.stat.freqItems(Seq("StockCode", "Quantity")).show()

   frame.select(monotonically_increasing_id()).show(10)*/

    /**
     * 处理字符串类型
     *
     * initcap:将给定字符串中空格分隔的每个单词首字母大写 参数：column
     *
     * lower:字符串转小写
     * upper：字符串转大写
     * lpad：参数：字符串列，长度，补足串（pad） 把字符串变成定长的字符串，不足在左边补pad,如果长度小于字符串列，那就进行截取
     * ltrim：删除左边的空格
     * rpad：参数：字符串列，长度，补足串（pad） 把字符串变成定长的字符串，在右边补pad，如果长度小于字符串列，那就进行截取
     * rtrim：删除右边的空格
     * trim：删除两侧空格
     *正则表达式：
     * 1.字符串替换
     * regexp_extrace :提取符合正则的字符串
     * regexp_replace：将符合正则的字符串进行替换
     *translate: 字符级操作，用给定字符串中替换掉所有出现的某字符串，
     *contains :检查字符串是否存在
     *
     * 传入变长序列：
     *  参数类型为：_*
     *
     */

    /*frame.select(initcap(col("Description"))).show(5)

    session.sql("select initcap(Description) FROM dfTable").show(2)

    frame.select(lpad(lit("hellowor"),10,"1")).show(2)
    frame.select(rpad(lit("helloword"),10,"2")).show(2)
    frame.select(ltrim(lit("  hellow  ")),rtrim(lit("  hellow  ")),trim(lit("  helloword "))).show(2)
    session.sql("select ltrim('  hellow000 '),rtrim(' hellow999 '),trim('  hellow88  '),lpad(' helloooo ',10,'22'),rpad('hhll',5,'33') from dfTable").show(2)

    val strings: Seq[String] = Seq("black", "white", "red", "green", "blue")*/
   //| 在正则中表达或 实现构造出下列value字符串 mkString TraversableOnce 的方法，
    // 不传参时直接相连，三个参数：start sep end
   /* strings.map(_.toUpperCase).mkString("|")
    println(strings.map(_.toUpperCase).mkString)
    println(strings.mkString("(", "|", ")"))
    val value: String = "BLACK|WHITE|RED|GREEN|BLUE"
    frame.select(regexp_replace(col("Description"),value,"COLOR"),col("Description")).show(2)
    session.sql("select regexp_replace('Description','BLACK|WHITE|RED|GREEN|BLUE','COLOR') AS color_clean,Description  from dfTable").show(2)
    //LEET-》1337 其中会将L-》1 E——》3 T——》7
    frame.select(translate(col("Description"),"LEEF","1337"),col("Description")).show(2)
   // session.sql("select translate(Description,'LEET','1337'),Description from dfTable")
  //提取第一个匹配到的值
    frame.select(regexp_extract(col("Description"),value,0),col("Description")).show(2)

    //动态创建人数量的列
    //实现在序列的最后添加* ,构造边长序列参数
    val columns: Seq[Column] = strings.map(color => {
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }) :+ expr("*")

   frame.select(columns:_*).where(col("is_white").or(col("is_red"))).select("Description").show(3)
     */
    /**
     * 处理日期和时间戳
     * TimestampType:只支持二级精度 到秒，处理毫秒或者微秒 需要将数据类型变成long
     * spark 使用java 的日期和时间戳
     * date_sub:start: Column, days: Int 获得start的days 之前的时间
     * date_add ：start: Column, days: Int 获得start 的days 之后的时间
     *datediff: 参数：end start 返回两个日期之间的天数 end-start
     *months_between :给出两个日期之间相隔的月数
     *to_date: 参数：column ,fmt(可选) 以指定格式将字符串转为日期数据，默认使用yyyy-MM-dd,无法解析日期时返回null
     *to_timestamp: 参数：column[string/date],fmt(可选)默认的时间格式时yyyy-MM-dd HH:mm:ss
     *
     *
     *
     */
    //range 创建一个列名为id  值为long 型的df
   /* val frame1: DataFrame = session.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())
    frame1.createOrReplaceTempView("dateTable")

    frame1.select(date_sub(col("today"),5),date_add(col("today"),5)).show(1)
    session.sql("select date_sub(today,5),date_add(today,5) from dateTable").show(1)
    //查看两个日期之间的时间间隔
    frame1.withColumn("week_ago",date_sub(col("today"),7)).select(datediff(col("week_ago"),col("today"))).show(1)
    session.sql("select  datediff(today,date_sub(today,7)) from dateTable").show(1)

    session.range(5).withColumn("date",lit("2017-01-01")).select(to_date(col("date"))).show(1)
    session.range(10).select(to_date(lit("2017-17-11"),"yyyy-dd-MM")).show(1)
    session.range(10).select(to_timestamp(lit("2017-11-17"))).show()
    session.range(10).select(to_timestamp(lit("2017-17-11"),"yyyy-dd-MM")).show()
    //date 与timestamp 转换 二者进行比较时需要使用同一种类型格式
    session.range(10).select(to_timestamp(to_date(lit("2017-17-11"),"yyyy-dd-MM"))).show()*/

    session.sql("select cast(to_date('2017-01-01','yyyy-dd-MM') as timestamp) ")

    /**
     * 处理空值
     * 当列被声明为不具有null 的值类型时，spark 不会强制拒绝空值的插入 ，但是会得到不正确的结果和奇怪的表达式
     * 1.显式删除null
     * 2.使用某实值代替null
     * coalesce: 从一组列中选择第一个非空值列
     * sql 函数：
     * ifnull:
     * nullif:如果两个值相等，返回null
     * nvl：如果第一个值为null，则返回第二个值，否则返回第一个
     * nvl2:如果第一个不为null,返回第二个，否则返回最后一个指定值
     * drop :删除包含null 的行
     *
     */
     /*frame.select(coalesce(col("Description"),col("CustomerId"))).show()

     session.sql("select coalesce('Description','CustomerId') from dfTable").show()*/

    session.sql("select nullif('value','value'),ifnull(null,'return_value'),nvl(null,'retrun_value'),nvl2(null,'retrun_value','else_value')" ).show()
    //出错了，bu'z不支持这种写法
    // frame.selectExpr("ifnull(null,'return_value'),nullif('value','value'),nvl(null,'retrun_value'),nvl2(null,'retrun_value','else_value')").show()

    /** frame.na
     *  DataFrameNaFunctions:Functionality for working with missing data in DataFrames.
     *  drop :无参=参数：any：删除任何含有null或者nan 的行 all:删除全部行为空的行
     *  drop:any/all,类名* 当特定列中为空时删除行
     *
     *  fill:用一组值填充一列或者多列 参数：替换值，列名 /map k-列名 v-替换值
     *replace:字符串替换
     *
     *
     *
     */
    /*frame.na.drop()
    frame.na.drop("all",Seq("StockCode", "InvoiceNo"))
    //sql 逐列进行判断删除
    //替换frame 中字符串列中空值为当前值
    frame.na.fill("all null values").show(2)
    //替换frame 中Integer 类型的列为当前值
    frame.na.fill(5,Seq("StockCode","InvoiceNo")).show(2)*/

    val map: Map[String, Any] = Map("StockCode" -> 5, "Description" -> "No Value")
    frame.na.fill(map).show(10,false)
    frame.na.replace("Description",Map(""->"UNKNOWN"))

    /**
     * 处理复杂数据类型
     * struct ：视为df 中嵌套的df
     * 创建结构体：（column1,column2） struct(column1,column2)
     * 查询：col("structname").getField("fieldname")/structname.* 查询出结构体中所有值
     * array :
     * 1. 将一列数据进行切分，将该列数据类型变成array[string]
     * 对array 类型的lie 数据可以执行size()求取数据长度 , array_contains 数组是否包含某个值
     * explode 将arrag中的每一行创建为一个值
     *
     * map：
     * 把两个列映射成为key-value 列
     * 查询：
     * map['key']  当key 不存在时返回null
     *
     * explode(map) 把map 类型的列变成两个列
     *
     * json:
     *1. to_json:把struct 变成json
     * 2.from_json:输入一个schema ,将jsonstruct  解析成列
     * 3.get_json_object:参数：json列 ，解析路径，返回值列
     *
     *
     */
    val frame1: DataFrame = frame.select(struct(col("Description"), col("InvoiceNo")).alias("complex"), col("*"))
    frame1.show(2)
    frame1.createOrReplaceTempView("compleDF")
    session.sql("select complex.* ,Description from compleDF").show(20 ,false)
    frame1.selectExpr("complex.Description").show(2)
    frame1.select(col("complex").getField("Description")).show(2)

    val frame2: DataFrame = frame.withColumn("array_column", split(col("Description"), " "))
    frame2.show()
    frame2.select(size(col("array_column")),array_contains(col("array_column"),"RED")).show()
    frame2.select(explode(col("array_column")),col("InvoiceNo")).show(50)
    session.sql("select * from (select split(Description,' ') as splitted ,*  from dfTable) lateral view explode(splitted) as exploded").show(50,false)
    //map 方法用不了，不知道为啥
    //frame.select(map(col("Description"), col("InvoiceNo")))

    val frame3: DataFrame = frame.withColumn("json_column", struct(col("InvoiceNo"), split(col("Description"), " ").alias("array")))
    frame3.select(col("json_column")).show(2,false)
    frame3.printSchema()
    /* new StructType(Array( StructField("InvoiceNo",StringType),
       new StructField("array",Array,true)))*/

    //frame3.select(from_json(col("json_column")))
    /**
     * 创建自定义函数
     * udf(): 创建的函数只能被df 使用
     * session.udf.register: 将udf 注册为sql 函数
     * spark 中可以使用hive的udf/udaf函数 需要开启hive 支持
     * SparkSession.builder().enableHiveSupport()
     *sql 中注册hive 的udf/udaf,指定依赖项
     * CREATE TEMPORARY FUNCTION myFunc AS 'com.organization.hive.udf.FunctionName'
     */

    val function2: UserDefinedFunction = udf(power3(_: Double), DoubleType)
    frame.select(function2(col("InvoiceNo").cast(DoubleType))).show(2)
    session.udf.register("power3",power3(_:Double):Double)
    session.sql("select function2(cast (InvoiceNo as double)) from dfTable").show(2)




  }


}
