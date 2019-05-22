package com.lifotech.loanstats

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * The class provides related to loan performances
  *
  */
object LoanPerformance {

  lazy val logger: Logger = Logger.getLogger(this.getClass.getName)

  lazy val APP_NAME = "LoanPerformance"
  lazy val FILE_NAME_NOT_PROVIDED_EXCEPTION_MSG = "The data file name file with absolute path must be provided."

  val TERM_TOKEN_POSITION: Int = 5 // term for example 60 months
  val INT_RATE_TOKEN_POSITION: Int = 6 // int rate
  val GRADE_TOKEN_POSITION: Int = 8 // grade

  val LOAN_AMOUNT: Int = 2
  val LOAL_STATUS: Int = 16; // such as "fully paid"
  val TOTAL_PAYMENT: Int = 40;
  val TOTAL_PRINCIPLE_RECEIVED = 42
  val TOTAL_INTEREST_RECEIVED_ = 43


  def main(args: Array[String]): Unit = {
    setupLogger();

    val fileName = args(0)
    if (fileName == null) {
      logger.error(FILE_NAME_NOT_PROVIDED_EXCEPTION_MSG)
      return
    }


    getTotalIssuedByGrade(2015, 4, 2015, 4)

  }


  def setupLogger(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    logger.setLevel(Level.INFO)
  }

  //TODO
  def getTotalIssuedByGrade(startYear: Int, startQuarter: Int, endYear: Int, endQuarter: Int): Unit = {
    val sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]")
    val sparkContext = SparkContext.getOrCreate(sparkConf)

    val sparkSession = SparkSession.builder().appName("LendingClubHistoricalReturnByGrade").config(sparkConf).config("spark.sql.warehouse.dir", SparkCommonUtil.tempDir).getOrCreate();


    val lendingClubDf: DataFrame = sparkSession.read.option("header", true).csv(SparkCommonUtil.dataDir + "LoanStats3d_securev1-sk.csv")


    // select name and salary
    lendingClubDf.select("loan_amnt", "grade", "issue_d")


    //val isTheDateBetweenStartAndEndQuarter: Boolean = isTheDateBetweenStartAndEndQuarter(lendingClubDf("issue_d").toString(), startYear, startQuarter, endYear, endQuarter)


    //lendingClubDf.filter(lendingClubDf("issue_d"))


    lendingClubDf.groupBy("grade").avg("loan_amnt")


  }

  private def isTheDateBetweenStartAndEndQuarter(monthYear: String, startQuarter: Int, startYear: Int, endQuarter: Int, endYear: Int) = {

    val CURRENT_CNETURY = "20" // ASSUMEND BASED ON MAX LOAN TERM WHICH IS 60 MOTHS. NEED TO BE REVISIT TO GET IT DYNAMICALLY
    //parse monthYear
    val tokens = monthYear.split("-")

    val issueDateYear = (CURRENT_CNETURY + tokens(1)).toInt
    val monthAsString = tokens(0)

    var issueDatequarter = 0;

    if (monthAsString.equalsIgnoreCase("Jan") || monthAsString.equalsIgnoreCase("Feb") || monthAsString.equalsIgnoreCase("Mar")) {
      issueDatequarter = 1
    }

    if (monthAsString.equalsIgnoreCase("Apr") || monthAsString.equalsIgnoreCase("May") || monthAsString.equalsIgnoreCase("Jun")) {
      issueDatequarter = 2
    }

    if (monthAsString.equalsIgnoreCase("Jul") || monthAsString.equalsIgnoreCase("Aug") || monthAsString.equalsIgnoreCase("Sep")) {
      issueDatequarter = 2
    }

    if (monthAsString.equalsIgnoreCase("Oct") || monthAsString.equalsIgnoreCase("Nov") || monthAsString.equalsIgnoreCase("Dec")) {
      issueDatequarter = 2
    }


    if (((issueDatequarter >= startQuarter && issueDatequarter <= endQuarter)) && ((issueDateYear == startYear) || (issueDateYear == endYear))) true
    else false

  }


}
