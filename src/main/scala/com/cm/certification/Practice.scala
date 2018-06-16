package com.cm.certification

import org.apache.spark.sql.SparkSession

object Practice {
   
  def main(args : Array[String]){
     
      val spark = SparkSession
                    .builder()
                    .appName("Practice")
                    .master("local")
                    .getOrCreate()
      //val textFile = spark.read.textFile("/Users/nisum/Desktop/exam/applefollowup/left-join/employee_pay.csv");
        //val employeePayDF = spark.read.csv("/Users/nisum/Desktop/exam/applefollowup/left-join/employee_pay.csv")
         val employeePayDF = spark.read.format("csv")
                                   .option("header", true)
                                   .option("inferSchema", true)
                                   .load("/Users/nisum/Desktop/exam/applefollowup/left-join/employee_pay.csv")
        //employeePayDF.show()
          employeePayDF.first()               
      
   }
}