package com.esen

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



/**
 * 聚合 kvrdd
 *  自定义分区
 *  rdd join
 *
 * ByKey -> PairRDD 类型 需要把rdd 映射为k-v 类型
 * 算子
 * keyBy:根据当前value 创建key f: T => K  返回 RDD(K,T)
 * mapValues：对于二元组，spark 将自动假设第一个是key,第二个是value 作用：对键值对每个value都应用一个函数，但是，key不会发生变化
 * keys:对键值对格式的数据提取key values:提取特定的value
 * lookup:查找某个key 对应的value,不强制输入只有一个键
 *
 *聚合操作：
 *
 *countByKey:计算每个key 对应的数据项的数量，并将结果写到driver ,因此适用于数据量小，如果数据量很大，使用rdd.mapValues(_ => 1L).reduceByKey(_ + _)
 *
 *
 *
 *
 * */


object KVRDDLearn {
  def main(args: Array[String]): Unit = {
     val conf: SparkConf = new SparkConf().setAppName("KVRDD").setMaster("local[3]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val myCollection = "Spark The Definitive Guide ： Big Data Processing MadeSimple".split(" ")
    val words = session.sparkContext.parallelize(myCollection,2)
    val value: RDD[(String, String)] = words.keyBy(word => word.indexOf(0).toString)
    val value1: RDD[(String, String)] = value.mapValues(word => word.toLowerCase)
    value.lookup("s")
    value.countByKey()
    value.mapValues(_=>1L).reduceByKey(_+_)

    /**
     * groupByKey 与reduceByKey 的比较：
     *  目的汇总每个key 的values
      */
    val kvcharacter = words.map(s=>(s,1))
    //方法一：
     kvcharacter.groupByKey()
  }
}
