package com.esen

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{approx_count_distinct, col, collect_list, collect_set, count, countDistinct, expr, first, grouping_id, last, max, min, to_date}
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, Row, SparkSession}

/**
 * 通过key对value列使用一个或者多个聚合函数进行列转换
 * 分组操作返回RelationalGroupedDataset 聚合操作在其上进行
 * count:
 * dataframe 中的count： 动作操作 进行统计数据集的大小，并且缓存dataframe 到内存
 * sql.function中的count 是一个转换操作，count(*) 会统计空值，count(col("")) 不统计空值
 *
 * countDistinct: 统计某列的唯一值数量
 * approx_count_distinct: 在处理大数据量时得到一个大约值来提升效率
 * first ：df 中第一行某列的值
 * last:df 中最后一行某列的值
 * max:df 中某数值列的最大值
 * min:df 中某数值列的最小值
 * sum:df 中某数值列的总和
 * avg：df 中某数值列的平均值
 * mean:df 中某数值列的平均值
 * var_pop:总体方差
 * var_samp：样本方差
 * stddev_pop：总体标准差
 * stddev_samp：样本标准差
 * skewness：偏度系数，数据对于平均值的不对称程度
 * kurtosis：峰度系数，数据分布形态的陡缓程度
 * corr:相关性
 * covar_samp：样本协方差
 * covar_pop：总体协方差
 *
 * 大多数聚合计算都支持对去重值进行计算，即：Agg_Func(distinct(col("")))
 *
 * 复杂类型聚合操作：
 *collect_set:将unique 唯一值收集到一个set 集合
 *collect_list:将一列值收集到list
 *
 *DataSet 与RelationalGroupedDataset
 *
 *
 */
object DFAggFunc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DFAggFunc").setMaster("local[3]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val value: Dataset[Row] = session.read.format("csv").option("header", true).option("inferSchema", true)
      .load("G:\\Spark-The-Definitive-Guide-master\\Spark-The-Definitive-Guide-master\\data\\retail-data\\all\\*.csv").coalesce(5)
    value.createOrReplaceTempView("dfTable")
   /* value.select(count(col("StockCode"))).show()
    value.select(count("*")).show()
    val l1: Long = System.currentTimeMillis()
    value.select(countDistinct("StockCode")).show()
    print(System.currentTimeMillis()-l1)
    val l2: Long = System.currentTimeMillis()
    value.select(approx_count_distinct("StockCode",0.1)).show()
    print(System.currentTimeMillis()-l2)
    value.select(first("InvoiceDate"),first("StockCode"),last("InvoiceDate"),last("StockCode"),max("InvoiceNo"),min("InvoiceNo")).show()

    value.select(collect_list("Country"),collect_set(col("Country"))).show()*/

    //val dataset: RelationalGroupedDataset = value.groupBy()
    //value.groupBy("InvoiceDate").agg("StockCode"->"count")
    // relationalGroupedDataset 中使用agg进行函数聚合操作，返回dataset
    //agg:column,map<string,string> 列名-> 函数名 expr:聚合表达式
    value.groupBy("InvoiceDate","StockCode").agg(expr("count(Country)")).explain()
    //计算一个客户有史以来最大购买数量
    val dfWithDate = value.withColumn("date", to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    //session.sql("select CustomerId,date,Quantity,rank() over(partition by CustomerId,date order by Quantity desc) from dfWithDate").show()

    //ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 从历史到当前的一个累计排序，就是把当前数据从之前窗口的数据也进行对比，和前面的结果没区别
    //session.sql("SELECT CustomerId, date, Quantity, rank() OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rank from dfWithDate").show()

    //session.sql("SELECT CustomerId, date, Quantity,first_value(Quantity) OVER (PARTITION BY CustomerId, date ORDER BY Quantity DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rank from dfWithDate").show()

    /**
     * UTF :多行输入对应多行输出
     * 窗口函数：
     * 根据分组排序，将一部分行划分为一个frame 这个frame 称为窗口 ，
     * 基于窗口进行的对其他列进行的统计分析 所用的聚集函数称为分析函数
     *窗口的划分：
     *
     * ROWS BETWEEN ... ADN
     * RANGE BETWEEN ... AND
     * UNBOUNDED PRECEDING /N（数字 eg 5） PRECEDING：无限制向前偏移（从第一行开始）/从当前行往前数N（eg 5） 行开始
     * CURRENT ROW :当前行 偏移量为0
     * UNBOUNDED FOLLOWING/N （数字 eg 5）FOLLOWING ：无限制向后偏移（到最后一行为止）/从当前行往后数n（eg 5） 行
     *
     * first_value :获得分析窗口中排序的第一行
     * last_value:获得分析窗口中排序的最后一行
     * lag:参数：列名，【行数 (默认1)，null时默认值】 获得当前行的向上第n行
     * lead :获得当前行的向下第n行
     */


    /**
     * grouping sets: 分组集, 对同一份数据进行多个分组的结果进行union 的简单表达
     * rollup: 实现层级钻取，以左侧的列为主列，实现由细分到汇总的过程
     * rollup(data,county)=>data,county 分组聚合结果=》data 分组聚合结果=》总数()
     *cube : 实现 所有参与的列值进行所有维度的全组合聚合
     *  cube(data,country)=>data,country,(data,country),()
     * GROUPING__ID/grouping_id
     * 指定分组聚合结果代表
     *
     */

    val dfNoNull = dfWithDate.drop()
    dfNoNull.createOrReplaceTempView("dfNoNullView")
    //统计每个用户各种股票数据及总数量
    //session.sql("select CustomerId, stockCode, sum(Quantity) from dfNoNullView group by CustomerId, stockCode grouping sets((CustomerId, stockCode),()) ").show(50)
    //所有日期交易的总股票数、每个日期交易的所有股票数，以及在每个日期中每个国家产生的股票交易数
    //dfNoNull.rollup("Date","Country").agg(expr("sum(Quantity) as total")).select("*").show()

    session.sql("select Date,Country ,sum(Quantity),GROUPING__ID from dfNoNullView group by Date,Country with rollup").show()

    //在所有日期和所有国家发生的交易总数。在每个日期发生于所有国家的交易总数。
    // 在每个日期发生于每个国家的交易总数。在所有日期发生于每个国家的交易总数。
   //出错了: grouping_id() can only be used with GroupingSets/Cube/Rollup 不能用在dataset 的select 函数里面
    dfNoNull.cube("Date","Country").agg(expr("sum(Quantity) as total"),grouping_id()).select(col("Date"),col("Country"),col("total"),col("grouping_id()")).show()


    session.sql("select Date,Country ,sum(Quantity),GROUPING__ID from dfNoNullView group by  Date,Country with cube").show()

    //行转列
    /**
     * pivot:
     * 根据某列结果数据，对数据值进行分组，同时分组数据值与数值列进行结合，对每个数值列进行聚合后产生一个新列
     *
     */

    dfWithDate.groupBy("date").pivot("Country").sum().where("date > '2011-12-05'").show()
     //自定义聚合


  }


}
