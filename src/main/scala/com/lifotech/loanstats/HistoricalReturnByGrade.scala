package com.lifotech.loanstats

import java.sql.DriverManager

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * The iniital class written as quick start to get the idea of problem.
  * It's here just for the historical or reference purpose.
  */
object HistoricalReturnByGrade {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val logger = Logger.getLogger(this.getClass.getName)
    logger.setLevel(Level.INFO)

    val tempDir = "/Users/sksingh/tmp"


    val sparkConf = new SparkConf().setAppName("LendingClubHistoricalReturnByGrade").setMaster("local[*]")
    val sparkContext =  SparkContext.getOrCreate(sparkConf)

    val sparkSession = SparkSession.builder().appName("LendingClubHistoricalReturnByGrade").config(sparkConf).config("spark.sql.warehouse.dir", tempDir).getOrCreate();


    val lendingClubDf: DataFrame = sparkSession.read.option("header", true).csv(SparkCommonUtil.dataDir + "LoanStats3d_securev1-sk.csv")

    lendingClubDf.printSchema()

    lendingClubDf.createOrReplaceTempView("lendingClubTempView")

    val  lendingClubProcesedDf = sparkSession.sql("select grade, loan_amnt, funded_amnt, substring(int_rate,0, length(int_rate) -1) int_rate,issue_d,  fico_range_high, fico_range_low,  loan_status, case when trim(term) ='60 months' THEN 1 When  trim(term) = '36 months' Then 2 Else 3 end as term   from lendingClubTempView")



    //val r = sc.makeRDD(1 to 4)

    lendingClubProcesedDf.foreachPartition {

      it =>

        val conn= DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","pragya")

        val del = conn.prepareStatement ("INSERT INTO lending_club (grade, loan_amnt, funded_amnt, int_rate, issue_d, fico_range_high, fico_range_low, loan_status, term) VALUES (?,?,?,?,?,?,?,?,?) ")

        for (record <-it) {

          //println(record(0))


        }

    }


    //val demoOf = sparkSession.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3306/test", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "Lending_Club", "user" -> "root", "password" -> "pragya")).load()

    //demoOf.show()






  }



  case class lending_club(grade  : String, loan_amnt: Integer, funded_amnt: Integer, int_rate:Double, issue_d:String, fico_range_high: Integer, fico_range_low: Integer, loan_status: Integer, term: String)

}
