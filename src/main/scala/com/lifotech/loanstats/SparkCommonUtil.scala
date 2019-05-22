package com.lifotech.loanstats

import org.apache.spark.{SparkConf, SparkContext}

/**
  * The common utility class based on the DRY and single point truth principle
  */
object SparkCommonUtil {
  val masterURL = "local[*]"
  val dataDir = "data/"
  val tempDir = "./tmp/"
  val sparkConf = new SparkConf()
  val sparkContext = SparkContext.getOrCreate(sparkConf)
}
