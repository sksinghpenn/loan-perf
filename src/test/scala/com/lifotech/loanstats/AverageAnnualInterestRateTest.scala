package com.lifotech.loanstats

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}


class AverageAnnualInterestRateTest extends FunSuite with Matchers {

  lazy val records: Array[String] = Array("68506789,,12000,12000,12000, 60 months,9.80%,253.79,B,B3,Teacher,10+ years,MORTGAGE,65000,Not Verified,Dec-15,Fully Paid,n,https://lendingclub.com/browse/loanDetail.action?loan_id=68506789,,debt_consolidation,Debt consolidation,660xx,KS,23.84,0,Nov-03,785,789,0,,,18,0,9786,13.40%,37,w,0,0,13842.62641,13842.63,12000,1842.63,0,0,0,Nov-17,8529.37,,Nov-17,819,815,0,,1,Individual,,,,0,0,181540,2,6,2,3,1,65001,61,1,5,7909,42,73200,0,0,0,8,10086,41055,17.1,0,0,127,145,4,1,2,4,,14,,0,2,4,5,10,13,11,22,4,18,0,0,0,3,100,0,0,0,291626,74787,49500,105910,,,,,,,,,,,,,,N,,,,,,,,,,,,,,,Cash,N,,,,,,",
    "68426545,,16000,16000,16000, 60 months,12.88%,363.07,C,C2,Senior Structural Designer,1 year,MORTGAGE,70000,Not Verified,Dec-15,Current,n,https://lendingclub.com/browse/loanDetail.action?loan_id=68426545,,debt_consolidation,Debt consolidation,786xx,TX,26.4,0,Feb-88,720,724,0,,,13,0,28705,56.30%,29,w,11038.45,11038.45,8321.99,8321.99,4961.55,3360.44,0,0,0,Dec-17,363.07,Jan-18,Dec-17,704,700,0,,1,Individual,,,,0,0,265836,0,2,0,2,13,33702,74,1,1,8739,64,51000,0,0,1,3,24167,17922,61,0,0,147,334,9,9,2,9,,11,,0,4,6,5,12,6,10,21,6,13,0,0,0,1,100,60,0,0,309638,62407,45900,45838,,,,,,,,,,,,,,N,,,,,,,,,,,,,,,Cash,N,,,,,,",
    "68394562,,30000,30000,30000, 36 months,15.77%,1051.31,D,D1,Vice-President,2 years,MORTGAGE,175000,Not Verified,Dec-15,Fully Paid,n,https://lendingclub.com/browse/loanDetail.action?loan_id=68394562,,home_improvement,Home improvement,430xx,OH,18.5,0,Aug-97,725,729,1,61,,9,0,21831,50.30%,23,w,0,0,33903.76574,33903.77,30000,3903.77,0,0,0,Dec-16,23456.38,,Sep-17,734,730,0,,1,Individual,,,,0,0,408118,2,4,2,4,6,130740,84,0,1,12141,71,43400,4,3,3,7,51015,21569,50.3,0,0,118,220,16,6,3,16,,0,61,0,2,2,3,7,9,4,11,2,9,0,0,0,3,95.7,33.3,0,0,447599,152571,43400,146568,,,,,,,,,,,,,,N,,,,,,,,,,,,,,,Cash,N,,,,,,"
  )

  val sparkConf: SparkConf = new SparkConf().setAppName("AverageAnnualInterestRateTest").setMaster("local[*]")
  val sparkContext = SparkContext.getOrCreate(sparkConf)
  val rdd = sparkContext.parallelize(records)

  test("test averageInterestRateByGrade") {
    val actualResult: Array[(String, Double)] = AverageAnnualInterestRate.averageInterestRateByGrade(rdd)

    actualResult.foreach(println(_))

    val expectedResult: Array[(String, Double)] = Array(("B", 9.800000190734863), ("C", 12.880000114440918), ("D", 15.770000457763672))

    assert(actualResult(0)._1.equals("B"))
    assert(actualResult(0)._2.equals(9.800000190734863))

    assert(actualResult(1)._1.equals("C"))
    assert(actualResult(1)._2.equals(12.880000114440918))


    assert(actualResult(2)._1.equals("D"))
    assert(actualResult(2)._2.equals(15.770000457763672))


  }


  test("test averageInterestRateByTerm") {
    val actualResult: Array[(String, Double)] = AverageAnnualInterestRate.averageInterestRateByTerm(rdd)

    actualResult.foreach(println(_))

    val expectedResult: Array[(String, Double)] = Array(("B", 9.800000190734863), ("C", 12.880000114440918), ("D", 15.770000457763672))

    assert(actualResult(0)._1.equals("36 months"))
    assert(actualResult(0)._2.equals(15.770000457763672))

    assert(actualResult(1)._1.equals("60 months"))
    assert(actualResult(1)._2.equals(11.34000015258789))


  }
}