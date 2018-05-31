package com.mc.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window



object product {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("EventsCountIntervalBased").getOrCreate()
   
       runQ5Example(spark)

       spark.stop()

  }
  
    private def runProductExample(spark: SparkSession): Unit = {
   
         import spark.implicits._
        
         case class Product(product_id: String, product_name: String, product_type: String, product_version: String, product_price: Int)
        
          // For implicit conversions from RDDs to DataFrames
          import spark.implicits._
          val ProductDF = spark.sparkContext
            .textFile("/Spark/src/main/resources/Product.txt")
            .map(_.split(","))
            .map(p => (p(0), p(1).trim(), p(2), p(3), p(4).replace("$", "").toInt)).toDF("product_id", "product_name", "product_type", "product_version", "product_price")
        
                    
            //registerTempTable is depricated method
            ProductDF.createOrReplaceTempView("product")
        
            // SQL statements can be run by using the sql methods provided by Spark
            spark.sql("SELECT product_id,product_name,product_type,product_version,product_price FROM product ORDER BY product_price DESC").show(5)
        
            ProductDF.sort($"product_price".desc).show(5)
    
   }
  
    private def runQ2Example(spark: SparkSession): Unit = {
   
            import spark.implicits._

            val vProduct = spark.sparkContext
                  .textFile("/Spark/src/main/resources/Product.txt")
                  .map(_.split('|')).map(p=>(p(0),p(1).trim(),p(2))).toDF("product_id","product_name","product_type")

            vProduct.createOrReplaceTempView("tblProduct")

            val vSales = spark.sparkContext
                .textFile("/Spark/src/main/resources/Sales.txt")
                .map(_.split('|')).map(s=>(s(2),s(4).replace("$","").toInt))
                .toDF("product_id","total_amount").groupBy("product_id").sum("total_amount").toDF("product_id","total_amount")
        
            vSales.createOrReplaceTempView("tblSales")

            // SPARK SQL
            spark.sql("SELECT product_name,product_type,SUM(total_amount) AS total_amount FROM tblProduct AS P INNER JOIN tblSales AS S ON P.product_id==S.product_id GROUP BY product_name,product_type ORDER BY product_name,product_type").show()

            // SPARK DATAFRAMES
            vProduct.join(vSales,vProduct("product_id")===vSales("product_id"),"inner").select($"product_name",$"product_type",vSales("total_amount")).sort($"product_name".asc).show()
   
    }
    
    private def runQ3Example(spark: SparkSession): Unit = {
   
        import spark.implicits._
           
        val vSalesDF = spark.sparkContext
              .textFile("/Spark/src/main/resources/Sales.txt")
              .map(_.split('|')).map(s=>(s(0),s(3),s(4).replace("$","").toInt)).toDF("transaction_id","timestamp","total_amount")
              
        val ts = to_date(unix_timestamp($"timestamp", "MM/dd/yyyy HH:mm:ss").cast("timestamp"))

        // Get the additional columns Year and Month
        val vSales=vSalesDF.withColumn("timestamp", ts).withColumn("Year",year(ts)).withColumn("Month",month(ts)) 
        vSales.createOrReplaceTempView("tblSales")
        
        val vRefund = spark.sparkContext
              .textFile("/Spark/src/main/resources/Refund.txt")
              .map(_.split('|')).map(s=>(s(0),s(1),s(5).replace("$","").toInt))
              .toDF("refund_id","original_transaction_id","refund_amount")

         vRefund.createOrReplaceTempView("tblRefundRaw")

         // SPARK SQL

         // If one transaction has multiple refunds, SUM refund amount by Transaction ID
         val tempRefundDS = spark.sql("SELECT original_transaction_id,SUM(refund_amount) AS refund_amount FROM tblRefundRaw GROUP BY original_transaction_id") 
         tempRefundDS.createOrReplaceTempView("tblRefund")

          spark.sql("SELECT Year,SUM(S.total_amount-IF(R.refund_amount is null,0,R.refund_amount)) AS Amount FROM tblSales S LEFT OUTER JOIN tblRefund R ON S.transaction_id = R.original_transaction_id WHERE Year = 2013 GROUP BY Year").show()

          // SPARK DATAFRAMES
          val vTotRefund = vRefund.groupBy("original_transaction_id").sum("refund_amount").toDF("original_transaction_id","refund_amount")
          vSales.join(vTotRefund,vSales("transaction_id")===vTotRefund("original_transaction_id"),"left_outer").filter($"Year"==="2013").withColumn("Amount",($"total_amount" - when($"refund_amount".isNull, 0).otherwise($"refund_amount"))).groupBy("Year").sum("Amount").toDF("Year","Amount").show()

    }
    
    private def runQ4Example(spark: SparkSession): Unit = {
   
        import spark.implicits._
            
        val vCustomer = spark.sparkContext
            .textFile("/Spark/src/main/resources/Customer.txt")
            .map(_.split('|')).map(s=>(s(0),s(1),s(2),s(3))).toDF("customer_id","first_name","last_name","phone_number")
        
        vCustomer.createOrReplaceTempView("tblCustomer")
      
        val vSalesDF = spark.sparkContext
            .textFile("/Spark/src/main/resources/Sales.txt")
            .map(_.split('|')).map(s=>(s(0),s(1),s(3),s(4).replace("$","").toInt))
            .toDF("transaction_id","customer_id","timestamp","total_amount")
      
        val ts = to_date(unix_timestamp($"timestamp", "MM/dd/yyyy HH:mm:ss").cast("timestamp"))
        // Get the additional columns Year and Month
        val vSales=vSalesDF.withColumn("timestamp", ts).withColumn("Year",year(ts)).withColumn("Month",month(ts)) 
        vSales.createOrReplaceTempView("tblSales")  
      
        val vRefund = spark.sparkContext
            .textFile("/Spark/src/main/resources/Refund.txt")
            .map(_.split('|')).map(s=>(s(0),s(1),s(5).replace("$","").toInt))
            .toDF("refund_id","original_transaction_id","refund_amount")
        
         vRefund.createOrReplaceTempView("tblRefundRaw")

        // SPARK SQL
        // If one transaction has multiple refunds, SUM refund amount by Transaction ID
        val tempRefundDS = spark.sql("SELECT original_transaction_id,SUM(refund_amount) AS refund_amount FROM tblRefundRaw GROUP BY original_transaction_id") 
        tempRefundDS.createOrReplaceTempView("tblRefund")

        val tempDataset = spark.sql("SELECT RANK() OVER (ORDER BY SUM(S.total_amount-IF(R.refund_amount is null,0,R.refund_amount)) DESC) AS Rank,first_name,last_name,phone_number,SUM(S.total_amount-IF(R.refund_amount is null,0,R.refund_amount)) AS Amount,Month,Year FROM tblCustomer C INNER JOIN tblSales S ON C.customer_id = S.customer_id LEFT OUTER JOIN tblRefund R ON S.transaction_id = R.original_transaction_id WHERE Month = 05 AND Year = 2013 GROUP BY first_name,last_name,phone_number,Month,Year")
        tempDataset.createOrReplaceTempView("tblMayTransactions")
        spark.sql("SELECT Rank, first_name,last_name,phone_number,Amount,Month,Year FROM tblMayTransactions WHERE Rank = 2").show()

        // SPARK DATAFRAMES
        val vTotRefund = vRefund.groupBy("original_transaction_id").sum("refund_amount").toDF("original_transaction_id","refund_amount")
        val vCustSales = vSales.join(vCustomer,vSales("customer_id")===vCustomer("customer_id"),"inner").select(vCustomer("customer_id"),$"first_name",$"last_name",$"phone_number",vSales("Month"),vSales("Year"),vSales("total_amount"),vSales("transaction_id")).toDF()

        vCustSales.join(vTotRefund,vSales("transaction_id")===vTotRefund("original_transaction_id"),"left_outer").filter($"Year"==="2013" && $"Month"==="05").withColumn("Amount",($"total_amount" - when($"refund_amount".isNull, 0).otherwise($"refund_amount"))).toDF().groupBy("first_name","last_name","phone_number","Month","Year").sum("Amount").toDF("first_name","last_name","phone_number","Month","Year","Amount").select(rank.over(Window.orderBy($"Amount".desc)).alias("Rank"),$"first_name",$"last_name",$"phone_number",$"Amount",$"Month",$"Year").filter($"Rank"==="2").show()

    }

    private def runQ5Example(spark: SparkSession): Unit = {
   
        import spark.implicits._
       
        val vProduct = spark.sparkContext
            .textFile("/Spark/src/main/resources/Product.txt")
            .map(_.split('|')).map(p=>(p(0),p(1).trim(),p(2))).toDF("product_id","product_name","product_type")
        
         vProduct.createOrReplaceTempView("tblProduct")

        val vSales = spark.sparkContext
            .textFile("/Spark/src/main/resources/Sales.txt")
            .map(_.split('|')).map(s=>(s(0),s(2))).toDF("transaction_id","product_id")
        
        vSales.createOrReplaceTempView("tblSales")

        // SPARK SQL
        spark.sql("SELECT DISTINCT product_name,product_type FROM tblProduct WHERE product_id NOT IN (SELECT DISTINCT product_id FROM tblSales) ORDER BY product_name,product_type").show()
      
        // SPARK DATAFRAMES
        vProduct.join(vSales,vProduct("product_id")===vSales("product_id"),"left_outer").filter($"transaction_id"==="null").select($"product_name",$"product_type").distinct.sort($"product_name".asc).show()

    }
    
    private def runQ6Example(spark: SparkSession): Unit = {
   
      import spark.implicits._
       
      val vCustomer = spark.sparkContext
            .textFile("/Spark/src/main/resources/Customer.txt")
            .map(_.split('|')).map(s=>(s(0),s(1),s(2),s(3))).toDF("customer_id","first_name","last_name","phone_number")

       vCustomer.createOrReplaceTempView("tblCustomer")
       
       val vSalesDF = spark.sparkContext
           .textFile("/Spark/src/main/resources/Sales.txt")
           .map(_.split('|')).map(s=>(s(0),s(1),s(2),s(3),s(4).replace("$","").toInt))
           .toDF("transaction_id","customer_id","product_id","timestamp","total_amount")

       // Converting string to timestamp 
        val ts = unix_timestamp($"timestamp", "MM/dd/yyyy HH:mm:ss").cast("timestamp")
        val vSales=vSalesDF.withColumn("timestamp", ts) 
        vSales.createOrReplaceTempView("tblSales")
        //vSales.show()

        // SPARK SQL
        val tempDataset = spark.sql("SELECT C.customer_id,first_name,last_name,product_id,LAG(product_id) OVER ( PARTITION BY C.customer_id ORDER BY timestamp) as prev_product_id,total_amount,timestamp FROM tblCustomer C INNER JOIN tblSales S ON C.customer_id = S.customer_id WHERE TO_DATE(timestamp) = date('2012-09-03') ORDER BY C.customer_id,timestamp")
        tempDataset.createOrReplaceTempView("tblTransactions")
        spark.sql("SELECT COUNT(DISTINCT customer_id) AS Customer_Count FROM tblTransactions WHERE product_id = prev_product_id").show()

        // SPARK DATAFRAMES
        val vtempDS = vCustomer.join(vSales,vCustomer("customer_id")===vSales("customer_id"),"inner").select(vCustomer("customer_id"),$"first_name",$"last_name",$"product_id",$"total_amount",$"timestamp").withColumn("prev_product_id", lag($"product_id", 1).over(Window.partitionBy(vCustomer("customer_id")).orderBy($"timestamp"))).toDF()

        vtempDS.filter($"product_id"===$"prev_product_id" && to_date($"timestamp")==="2012-09-03").select($"customer_id").distinct.agg(count("customer_id").alias("Customer_Count")).show()


        val vCustomer_Extended = spark.sparkContext
              .textFile("/Spark/src/main/resources/Customer_Extended.txt")
              .map(_.split('|')).map(s=>(s(0),s(1),s(2),s(3),s(4),s(5),(s(6),s(7),s(8),s(9),s(10)),(s(11),s(12),s(13),s(14),s(15)),(s(16),s(17),s(18),s(19),s(20)),s(21),s(22),s(23),s(24),s(25)))

              // Seperate Personal Information and create DF and then Table 

              val personal_info=vCustomer_Extended.map(p=>(p._1,p._2,p._3,p._4,p._5,p._6,p._10,p._11,p._12,p._13,p._14)).toDF("id","first_name","last_name","home_phone","mobile_phone","gender","personal_email_address","work_email_address","twitter_id","facebook_id","linkedin_id")

              personal_info.createOrReplaceTempView("tblPersonal_info")
 
                // Seperate Current_Address information and create DF then Table 

              val current_add=vCustomer_Extended.map(r=>(r._1,r._7)).map({case(a,(b,c,d,e,f))=>(a,b,c,d,e,f)}).toDF("id","current_street_address","current_city","current_state","current_country","current_zip")
 
                current_add.createOrReplaceTempView("tblcurrent_add")

              // Seperate Permanent_Address information and create DF then Table 

              val perm_add=vCustomer_Extended.map(t=>(t._1,t._8)).map({case(a,(b,c,d,e,f))=>(a,b,c,d,e,f)}).toDF("id","permanent_street_address","permanent_city","permanent_state","permanent_country","permanent_zip")
 
                perm_add.createOrReplaceTempView("tblperm_add")

              // Seperate Office_Address information and create DF then Table

              val office_add=vCustomer_Extended.map(u=>(u._1,u._9)).map({case(a,(b,c,d,e,f))=>(a,b,c,d,e,f)}).toDF("id","office_street","office_city","office_state","office_country","office_zip")
 
                office_add.createOrReplaceTempView("tbloffice_add")


              // SPARK SQL
              spark.sql("SELECT PI.id,PI.first_name,PI.last_name,PI.home_phone,PI.mobile_phone,PI.gender,CA.current_street_address,CA.current_city,CA.current_state,CA.current_country,CA.current_zip,PA.permanent_street_address,PA.permanent_city,PA.permanent_state,PA.permanent_country,PA.permanent_zip,OA.office_street,OA.office_city,OA.office_state,OA.office_country,OA.office_zip,PI.work_email_address,PI.twitter_id,PI.facebook_id,PI.linkedin_id FROM tblPersonal_info PI INNER JOIN tblcurrent_add CA ON PI.id=CA.id INNER JOIN tblperm_add PA ON PA.id=PI.id INNER JOIN tbloffice_add OA ON OA.id=PI.id WHERE CA.current_street_address = '1154 WINTERS Blvd'").show()
 
                // SPARK DATAFRAMES
              val Customer=personal_info.join(current_add,personal_info("id")===current_add("id"),"inner").join(perm_add,personal_info("id")===perm_add("id"),"inner").join(office_add,personal_info("id")===office_add("id"),"inner").filter($"current_street_address"==="1154 WINTERS Blvd").select(personal_info("id"),$"first_name",$"last_name",$"home_phone",$"mobile_phone",$"gender",$"current_street_address",$"current_city",$"current_state",$"current_country",$"current_zip",$"permanent_street_address",$"permanent_city",$"permanent_state",$"permanent_country",$"permanent_zip",$"office_street",$"office_city",$"office_state",$"office_country",$"office_zip",$"personal_email_address",$"work_email_address",$"twitter_id",$"facebook_id",$"linkedin_id").show()

    }
    
    private def runQ7Example(spark: SparkSession): Unit = {
   
        import spark.implicits._
        
        case class salesSchema(transaction_id:String, customer_id: String, product_id: String, timestamp: String,total_amount:String,total_quantity :String)
      
        val salesRawData = spark.sparkContext
            .textFile("/Spark/src/main/resources/Sales.txt")

        //salesRawData.take(5)

        val sales = salesRawData.map(x => x.split(',')).map( row => salesSchema(row(0), row(1), row(2), row(3), row(4).replace("$",""),row(5)) )

    }
    
    private def runQ8Example(spark: SparkSession): Unit = {
   
        import spark.implicits._
    }
}
