package com.esen

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * DataFrameStatFunctions : 包含统计相关的函数
 * DataFrameNaFunctions ：空值处理办法
 *
 *
 *
 */
object DataFrameDisposeDifferentDataType {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dataframelearndispose").setMaster("local[3]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //session.read.format("csv")



  }


}
