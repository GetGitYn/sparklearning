package com.esen

import org.apache.spark.Partitioner

class DomainPartitioner extends Partitioner {
  def numPartitions = 3
  def getPartition(key: Any): Int = {
    val customerId = key.asInstanceOf[Double].toInt
    if (customerId == 17850.0 || customerId == 12583.0) {
      return 0
    } else {
      return new java.util.Random().nextInt(2) + 1
    }
  }
}
