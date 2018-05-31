package com.mc.spark

// importing required libraries

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._

/*
   input is joblogdata = {time,ipaddress,url,duration,startTime,endTime}
   
   sample Input 
   ============
   
09:10 AM,191.12.65.76,url1,06 min,09:04 AM,09:10 AM
09:11 AM,191.12.65.77,url2,09 min,09:02 AM,09:11 AM
10:35 AM,191.12.65.78,url3,05 min,10:30 AM,10:35 AM
11:25 AM,191.12.65.79,url4,25 min,11:00 AM,11:25 AM
11:00 AM,191.12.65.80,url5,30 min,10:30 AM,11:00 AM
11:24 AM,191.12.65.81,url6,24 min,11:00 AM,11:24 AM

   to generate  graph report on x-axis is time intervals and y-axes is minutes per hour 
   output is to identify the duration will come under which time interval
   Ex: For example if we take interval is 5 minites will get 12 intervals for 60 minites
   0-5,6-10,11-15,16-20,21-25,26-30,31-35,36-40,41-45,46-50,51-55,56-60.
      
   
  Time(HH:mm),Duration, Time(Intervals(Duration will fall in any one of the intervals)
  
  Sample output
  
+-----+--------+--------+
| time|Duration|Interval|
+-----+--------+--------+
|09:10|      06|    6:10|
|09:11|      09|    6:10|
|10:35|      05|     0:5|
|11:25|      25|   21:25|
|11:00|      30|   26:30|
|11:24|      24|   21:25|
+-----+--------+--------+
      
  
*/

object EventInterval {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("EventsCountIntervalBased").getOrCreate()

    runProgrammaticSchemaExample(spark)

    spark.stop()
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    // Create an RDD
    val logRDD = spark.sparkContext.textFile("/Spark/src/main/resources/tt.txt")

    // define the schema for logRDD
    // create schema using programatically (or) case class (or) specifying directly toDF() method of dataFrame.
    // The schema is encoded in a string
   
    val schemaString = "time count"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // case class logSchema(time: String, Duration: Integer)

    // converting string to Time
    // used Time conversion using SimpleDateFormat java function
    // used trim and replace functions
    
    val inputFormat = new SimpleDateFormat("HH:mm")
    val outputFormat = new SimpleDateFormat("HH:mm")

    // loading raw data to RDD using textFile method
    // spliting each column   map with split
    // attaching to schema    pass the row values to case constructor and through map function
    // converting RDD to data frame using toDF function
   
    val rowRDD = logRDD
      .map(_.split(","))
      .map(attributes => Row(outputFormat.format(inputFormat.parse(attributes(0))), attributes(3).replace("min", "").trim))

    // Create data frame
    val logDF = spark.createDataFrame(rowRDD, schema)

    // Here required extra calculated column for interval identification
    
    // Here created user defined function to calculate time interval for given duration
    
    val getInterval: (String => String) = (duration: String) => {
      // used scala range array to hold 12 intervals  
      val arr = 5 to 60 by 5
      var low = 0
      // lenth of the range array 	
      var high = arr.length
      var du: Int = duration.toInt
      var strRange = ""
      // loop throuh until low greater than high
      while (low < high) {
          // first if condition this is only for first position of the value
        if ((du == arr(0)) || (du < arr(0))) {
          strRange = "0:" + arr(0)
        } else {
          // duration greater than lower interval and less than higher interval then say duration fall in b/w ranges
          if ((du > arr(low)) && (du <= arr(low + 1))) {
            strRange = (arr(low) + 1) + ":" + arr(low + 1)
          }
        }
        low = low + 1
      }
      strRange
    }

    // register the creatred function with udf to spark sql ,udf belongs to spark sql functions
    val sqlfunc = udf(getInterval)

    // applied the udf to dataframe and created calculated column TimeInterval by passing duration
    logDF.withColumn("Interval", sqlfunc(col("count"))).show()

  }

}