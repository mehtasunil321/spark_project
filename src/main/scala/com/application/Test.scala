package com.application

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]) = {
    println("first scala program")

    //val spark = initSparkSession
    val sparkSession = SparkSession.builder().master("local[*]").appName("first spark program").getOrCreate()

    val testDf = sparkSession.createDataFrame(Seq(("1234", "name1", 32),("1235", "name2", 33))).toDF("Id", "Name", "age")
  //testDf.show(5, false)
    testDf.write.mode("overwrite").csv("C:/Users/mehta/OneDrive/Desktop/TEST_OUTPUT/students.csv")
  }


  def initSparkSession = {
    SparkSession.builder().master("local[*]").appName("first spark program").getOrCreate()
  }


}
