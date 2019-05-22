package com.lifotech.loanstats

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
  * The class provides method to get average annual interest rate
  */
object AverageAnnualInterestRate {

  lazy val logger: Logger = Logger.getLogger(this.getClass.getName)

  lazy val APP_NAME = "AverageAnnualInterestRate"
  lazy val FILE_NAME_NOT_PROVIDED_EXCEPTION_MSG = "The data file name file with absolute path must be provided."

  val TERM_TOKEN_POSITION: Int = 5 // term for example 60 months
  val INT_RATE_TOKEN_POSITION: Int = 6 // int rate
  val GRADE_TOKEN_POSITION: Int = 8 // grade


  /**
    * main method called from the spark-submit job
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    setupLogger();

    val fileName = args(0)
    if (fileName == null) {
      logger.error(FILE_NAME_NOT_PROVIDED_EXCEPTION_MSG)
      return
    }


    try {
      val avgIntRateByGrade: Array[(String, Double)] = averageInterestRateByGrade(fileName)

      logger.info("***AVERANGE INTEREST RATE BY GRADE***")
      avgIntRateByGrade.foreach(logger.info(_))
      logger.info("****************************")

      val avgIntRateByTerm: Array[(String, Double)] = averageInterestRateByTerm(fileName)

      logger.info("***AVERANGE INTEREST RATE BY TERM***")
      avgIntRateByTerm.foreach(logger.info(_))
      logger.info("****************************")
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
  }


  /**
    * Sets up the logger
    */
  def setupLogger(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    logger.setLevel(Level.INFO)
  }

  /**
    * The method parses a tuple to return option of key and tuple
    * @param tuple containing term and rate
    * @return Option
    */
  private def  parse(tuple: (String, String)): Option[(String, (Float, Int))] = {

    if ((tuple._1.length == 0) || (tuple._2.length == 0)) None
    else Some((tuple._1, (tuple._2.substring(0, tuple._2.length - 1).toFloat, 1)))
  }

  /**
    * Gets the average interest rate by grade
    * @param fileName
    * @return Array of tuples
    */
  def averageInterestRateByGrade(fileName: String): Array[(String, Double)] = {
    val termRateRDDCleaned: RDD[String] = getCleanedRDD(fileName)

    val termRateRDD: RDD[(String, String)] = termRateRDDCleaned.map(line => (line.split(',')(GRADE_TOKEN_POSITION).trim, line.split(',')(INT_RATE_TOKEN_POSITION).trim))

    val termRateMapRDD: RDD[(String, (Float, Int))] = termRateRDD.map(line => (line._1, (line._2.substring(0, line._2.length - 1).toFloat, 1)))

    val termRateSumAndCountMapRDD: RDD[(String, (Float, Int))] = termRateMapRDD.reduceByKey((x: (Float, Int), y: (Float, Int)) => ((x._1 + y._1), (x._2 + y._2)))

    val result: RDD[(String, Double)] = termRateSumAndCountMapRDD.map(x => (x._1, x._2._1 / x._2._2))

    result.sortByKey().collect()
  }

  /**
    * Gets the average interest rate by grade
    * @param termRateRDDCleaned
    * @return Array of tuples
    */
  def averageInterestRateByGrade(termRateRDDCleaned: RDD[String]): Array[(String, Double)] = {

    val termRateRDD: RDD[(String, String)] = termRateRDDCleaned.map(line => (line.split(',')(GRADE_TOKEN_POSITION).trim, line.split(',')(INT_RATE_TOKEN_POSITION).trim))

    val termRateMapRDD: RDD[(String, (Float, Int))] = termRateRDD.map(line => (line._1, (line._2.substring(0, line._2.length - 1).toFloat, 1)))

    val termRateSumAndCountMapRDD: RDD[(String, (Float, Int))] = termRateMapRDD.reduceByKey((x: (Float, Int), y: (Float, Int)) => ((x._1 + y._1), (x._2 + y._2)))

    val result: RDD[(String, Double)] = termRateSumAndCountMapRDD.map(x => (x._1, x._2._1 / x._2._2))

    result.sortByKey().collect()
  }

  /**
    * Returns average interest rate by term
    * @param fileName
    * @return Array of tuple
    */
  def averageInterestRateByTerm(fileName: String): Array[(String, Double)] = {
    val termRateRDDCleaned: RDD[String] = getCleanedRDD(fileName)

    val termRateRDD: RDD[(String, String)] = termRateRDDCleaned.map(line => (line.split(',')(TERM_TOKEN_POSITION).trim, line.split(',')(INT_RATE_TOKEN_POSITION).trim))

    val termRateMapRDD: RDD[(String, (Float, Int))] = termRateRDD.map(line => (line._1, (line._2.substring(0, line._2.length - 1).toFloat, 1)))

    val termRateSumAndCountMapRDD: RDD[(String, (Float, Int))] = termRateMapRDD.reduceByKey((x: (Float, Int), y: (Float, Int)) => ((x._1 + y._1), (x._2 + y._2)))

    val result: RDD[(String, Double)] = termRateSumAndCountMapRDD.map(x => (x._1, x._2._1 / x._2._2))

    result.sortByKey().collect()
  }

  /**
    * Returns average interest rate by term
    * @param termRateRDDCleaned
    * @return Array of tuples
    */
  def averageInterestRateByTerm(termRateRDDCleaned: RDD[String]): Array[(String, Double)] = {
    val termRateRDD: RDD[(String, String)] = termRateRDDCleaned.map(line => (line.split(',')(TERM_TOKEN_POSITION).trim, line.split(',')(INT_RATE_TOKEN_POSITION).trim))

    val termRateMapRDD: RDD[(String, (Float, Int))] = termRateRDD.map(line => (line._1, (line._2.substring(0, line._2.length - 1).toFloat, 1)))

    val termRateSumAndCountMapRDD: RDD[(String, (Float, Int))] = termRateMapRDD.reduceByKey((x: (Float, Int), y: (Float, Int)) => ((x._1 + y._1), (x._2 + y._2)))

    val result: RDD[(String, Double)] = termRateSumAndCountMapRDD.map(x => (x._1, x._2._1 / x._2._2))

    result.sortByKey().collect()
  }


  /**
    * This is used to clean or validate the data
    * @param dataFileName
    * @return RDD of String
    */
  private def getCleanedRDD(dataFileName: String): RDD[String] = {
    try {

      val sparkConf: SparkConf = SparkCommonUtil.sparkConf.setAppName(APP_NAME)

      val sparkContext = SparkCommonUtil.sparkContext

      val lendingClubFullDataWithHeaderRDD = sparkContext.textFile(dataFileName)

      val header = lendingClubFullDataWithHeaderRDD.first()

      logger.info("HEADER RECORD : " + header)

      val lendingClubFullDataRDD = lendingClubFullDataWithHeaderRDD.filter(line => !line.equals(header))

      val firstRecord = lendingClubFullDataRDD.first()

      logger.info("FIRST RECORD : " + firstRecord)

      // used in data validation to find if each record is a valid record
      val tokens = firstRecord.split(',')


      // assuming that the first record is valid in terms of number of tokens
      logger.info("TOTAL TOKENS IN THE RECORD:" + tokens.length)

      val countOfToken = tokens.length

      lendingClubFullDataRDD.filter(line => line.split(',').length == countOfToken && line.split(',')(GRADE_TOKEN_POSITION).trim.length > 0 && line.split(',')(INT_RATE_TOKEN_POSITION).trim.length > 0)
    } catch {
      case e: Exception => {
        throw e
      }
    }

  }

}
