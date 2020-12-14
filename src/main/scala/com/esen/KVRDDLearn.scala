package com.esen

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random



/**
 *  rdd 当满足 k,v 格式时会被隐式转换成为 pairrdd ,XXbykey  这些方法返回rdd
 *  * 聚合 kvrdd kv
 *  自定义分区
 *  rdd join
 *
 * XXByKey -> PairRDD 类型 需要把rdd 映射为k-v 类型
 * 算子
 * keyBy:根据当前value 创建key f: T => K  返回 RDD(K,T)
 * mapValues：对于二元组，spark 将自动假设第一个是key,第二个是value 作用：对键值对每个value都应用一个函数，但是，key不会发生变化
 * keys:对键值对格式的数据提取key values:提取特定的value
 * lookup:查找某个key 对应的value,不强制输入只有一个键
 *
 *聚合操作：
 *
 *countByKey:计算每个key 对应的数据项的数量，并将结果写到driver ,因此适用于数据量小，如果数据量很大，使用rdd.mapValues(_ => 1L).reduceByKey(_ + _)
 *aggregateByKey: 参数：初始值，分组中每个key的value执行的函数，分组间每个key 对应的value 执行的函数
 *combineByKey: 调用 combineByKeyWithClassTag 参数 ：func a:createfunc u->c 转变values 类型指定combiner 输出类型 func b :mergevalue 合并map 端数据 func c:combine 合并函数
 * foldByKey:比 reduceByKey 多一个初始值，其他一样
 * CoGroups:根据key 组合rdd values  类似连接
 *
 *
 * 连接操作：
 * 内连接：join
 * 全外连接 ：fullOuterJoin
 * 左外连接：leftOuterJoin
 * 右外连接：rightOuterJoin
 * 笛卡儿积连接：cartesian 危险操作
 *
 *
 * zip:组合rdd  要求rdd 内元素个数与分区数相同 结果 pairRDD k-v 调用zip 的为key
 *
 * 控制分区：
 *
 * coalesce ：默认不开启shuffle 作用：折叠同一工作节点上的分区，以便在重新 分区时避免shuffle
 * 比如1000个分区对应到10个分区，每个分区对应父rdd 的 10个分区 没有shuffle
 * 如果最终结果只有一个分区，出于并行度最好设置shuffle 为true
 *
 * repartition:对数据进行重分区 跨节点分区执行shuffle  调用coalesce 调用shuffle为true
 * 分区增加时，只能使用repartition
 * 对于map 和fileter 方法而言，增加分区可以提高并行度，
 * 因为map 和fileter方法 本质调用的时mapPartition
 *
 * partitionBy:实现自定义分区
 * spark: new  HashPartitioner(10) 离散与new RangePartitioner() 连续
 *
 *
 *
 *
 * */


object KVRDDLearn {
  def createCombiner=(s:Int)=>{
    s
  }
  def mergevalues=(s:Int,c:Int)=>{

  }
  // 隐式方法，完成list 之间 的隐式比较，且前者总是大于后者
  implicit def t[A](x:List[A]):Ordered[List[A]]=new Ordered[List[A]]{def compare(that:List[A]):Int=1 }
  def main(args: Array[String]): Unit = {
     val conf: SparkConf = new SparkConf().setAppName("KVRDD").setMaster("local[3]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val myCollection = "Spark The Definitive Guide ： Big Data Processing MadeSimple".split(" ")
    val words = session.sparkContext.parallelize(myCollection,2)
    val wordcoa = session.sparkContext.parallelize(myCollection,7)
    val value: RDD[(String, String)] = words.keyBy(word => word.indexOf(0).toString)
    val value1: RDD[(String, String)] = value.mapValues(word => word.toLowerCase)
    value.lookup("s")
    value.countByKey()
    value.mapValues(_=>1L).reduceByKey(_+_)

    /**
     * groupByKey 与reduceByKey 的比较：
     *  目的汇总每个key 的values
     *
     *
      */
    val kvcharacter = words.map(s=>(s,1))

    //方法一：
    kvcharacter.groupByKey().foreach(row => (row._1, row._2.reduce(_ + _)))
    //问题： groupByKey  执行器在执行函数之前需要在内存中保存每个key 对应的所有values，当数据发生倾斜时，可能出现outOfMemory
    //方法二：
    /**
     * reduceByKey
     * reduceByKey 发生在每一个分组，并且不会把所有内容放入内存，没有shuffle
     */
    kvcharacter.reduceByKey(_ + _).collect().foreach(s=>print(s))
    //aggregateByKey
   kvcharacter.aggregateByKey(0)(_ + _, _ + _).collect().foreach(s=>print(s))

    //kvcharacter.combineByKey(s=>s,(s,c)=>(s+c),_+_)
    // foldByKey 相比reduceByKey 多一个初始值
    kvcharacter.foldByKey(3)((s,a)=>(s*a))
    val kvcharacter1 = words.map(s=>(s,new Random().nextBoolean()))
    kvcharacter.cogroup(kvcharacter1)
      .foreach(f=>{
        print(f._1)
        f._2._2.foreach(println)
      f._2._1.foreach(println)
      }
      )
    System.out.println(List(1,2,3)<List(1,2,3))
    wordcoa.coalesce(2).foreachPartition(s=>println(s));


    val df = session.read.option("header", "true").option("inferSchema", "true")
      .csv("G:\\Spark-The-Definitive-Guide-master\\Spark-The-Definitive-Guide-master\\data\\retail-data\\all")
    val rdd = df.coalesce(10).rdd
    val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)
         keyedRDD.partitionBy(new DomainPartitioner).map(_._1)
           .glom().map(_.toSet.toSeq.length).take(5)



  }
}
