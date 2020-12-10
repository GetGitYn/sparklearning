package com.esen

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 低级api:rdd,共享变量或者累加器
  * 入口：SparkContext
  * rdd:只读不可变的，且已经分块的，可被并行处理的，记录集合 rdd存放对象
  * 优点：可以完全控制，任何格式存储任何内容
  * 缺点：无论什么任务都得从底层开始，需要自己写优化代码
  * 即：除非有非常明确的理由，不然不要创建rdd 如：自定义分区
  * 常用rdd类型：通用型，k-v rdd
  * rdd 5个主要内部属性：数据分片列表，作用在每个数据分片的计算函数，
  * 与其他rdd依赖，k-v类型rdd分片方法，分片位置处理优先级
  *
  * testFile  读取文件，将文件内容转为rdd,文件每行一个row 对象  paramter:文件路径
  * wholeTextFiles  读取文件转成rdd,rdd 内容为 <文件名对象，字符串对象(文件内容)> paramter:文件路径
  *  filter  根据特定条件过滤数据集，true or false    paramter: f: T => Boolean 按照分区数进行
  *  distinct 去重  无参
  *   map 对输入数据集中的每一条记录执行转换方法返回一个新的rdd  paramter:f: T => U
  *   flatmap 返回一个 TraversableOnce 对象，`Iterator` and `Traversable 公共特性和方法 paramter:f: T => TraversableOnce[U]
  *   sortBy  获取到rdd中的某个值，对该值进行排序，返回对该值排序后的rdd   paramter:  f: (T) => K ,排序key 方法，升序还是降序，分区
  *   默认升序，创建一个k,v 二元组之后进行排序 分区好像没区别
  *   randomsplit 将一个rdd 切分为多个，返回一个包含rdd的数组
 *   groupBy :不建议使用，内里使用的还是groupByKey ,
  *
  *   action 算子：
  *   reduce : 将rdd 中任何类型的值规约为一个值 参数： f(a T,b T)=>T
  *   count : 根据某种函数计算符合条件的数据个数 paramter:f(a A)=>boolean
  *   countByValue  对rdd 中的值的个数进行计数 将结果集驱动到内存
 * first :返回数据集中的第一个值
 * * take :从rdd 中读取一定量的值 具体执行流程是：首先扫描一个数
 * * 据分区，然后根据该分区的实际返回结果的数量来预估还需要再读取多少个分区
 * * takeOrderd:默认升序
 * * top:
 * * 按默认排序返回前几个值时，top 函数与 takeOrdered 函数返
 * * 回值的排序顺序相反
 * *
 * * saveAsTextFile:将rdd 写入分区，遍历分区将分区的内容写入文件
 * * saveAsObjectFile :写入二进制文件 sequenceFile
 * * cache:默认只对内存中的数据进行缓存 缓存级别包括：memory only 仅内存 disk only 仅磁盘  off heap 堆外内存
 * * checkpoint:dataframe api 没有检查点的概念 将rdd 保存到磁盘
 * * pipe :利用管道技术，将分区中的数据作为管道的数据，以管道的输出的每一行作为分区的每一个元素，空分区也会调用外部进程
 * * mapPartition: 参数：函数 f Iteror[U]=>Iteror[U] 每次处理一个分区的数据 map实际为mapPartition 基于行操作的情况
 * * 我们能按照每个分区进行操作，处理单元为整个分区。在 RDD 的整个子数据集上执
 * * 行某些操作很有用，你可以将属于某类的值收集到一个分区中，或分组到一个分区上，然后对整个分组进行操作
 * *
 * * mapPartitIonWithIndex:参数：index ：分区索引，分区数据迭代器  标志数据集中每条数据的位置
 * * foreachPartition :迭代所有分区数据，适合数据库写入
 */

object rddlearn1 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setAppName("rddlearn1").setMaster("local[3]")
    val sparkContext=new SparkContext(sparkconf)
    val sparkSession=SparkSession.builder().config(sparkconf).getOrCreate()
    sparkSession.range(10).toDF().rdd.map(row=>row.getLong(0)).collect()
    sparkSession.range(10).rdd.flatMap(a=>Seq.apply(a*10)).foreach(a=>{print(a.toString);printf("aaa")})
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    sparkContext.wholeTextFiles("G:\\Spark-The-Definitive-Guide-master\\Spark-The-Definitive-Guide-master\\data\\bike-data\\201508_station_data.csv").foreach(s=>printf(s._1.concat(s._2.toString)))
    sparkContext.parallelize(myCollection,2).sortBy(s=>s).collect().foreach(s=>print(s))
    sparkContext.parallelize(myCollection,1).sortBy(s=>s).collect().foreach(s=>print(s))
   val testrdd=sparkContext.parallelize(myCollection,1);
    testrdd.reduce((a,b)=>a.concat(b)).count(a=>a.isDigit)

    val words: RDD[String] = sparkSession.sparkContext.makeRDD(myCollection)
    val value: RDD[String] = sparkSession.sparkContext.parallelize(myCollection,2)
    println(words.first())
    println(value.first())
    words.take(5)
    // 返回一个string 数组
    words.takeOrdered(5)
    words.top(5).foreach(s=>println(s))
    //words.saveAsTextFile("")
    //words.saveAsObjectFile("")
    //这个算子用出错了，再看
    words.pipe("sed -i 's#spark#hhh#g'").collect()
    words.mapPartitions(s=>Iterator[Int](1)).foreachPartition(s=>println(s))
    val value1: RDD[(Array[Char], Iterable[String])] = words.groupBy(s => s.toCharArray)
    value.foreachPartition(iter=>{
      val i = new Random().nextInt()
      val writer = new PrintWriter(new File(s"/tmp/random-file-${i}.txt"))
      while(iter.hasNext){
        writer.write(iter.next())
      }
      writer.close()
    })

  }


}
