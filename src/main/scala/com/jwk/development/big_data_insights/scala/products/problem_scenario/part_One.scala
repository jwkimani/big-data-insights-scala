package com.jwk.development.big_data_insights.scala.products.problem_scenario

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class part_One {
  val spark: SparkSession = SparkSession.builder.appName("products_application").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
  val sparkContext = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)


  /**
    * Solution to part one of products problem scenario
    *
    * @param path file path to products.csv file
    */
  def part_One_Solution(path: String): Unit = {
    //val tab_delimited_Header= "productID\tproductCode\tname\tquantity\tprice\tsupplierid"
    //val comma_delimited_Header= "productID,productCode,name,quantity,price,supplierid"

    //define schema of csv file
    val schema =
      StructType(
        Array(
          StructField("productID", IntegerType, false),
          StructField("productCode", StringType, false),
          StructField("name", StringType, false),
          StructField("quantity", IntegerType, false),
          StructField("price", FloatType, false)
        )
      )

    //read csv file from directory path using schema
    val productDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").option("inferSchema", "false").schema(schema).load(path)
    //show first 10 records in the dataframe
    productDF.show(10)

    // Register the DataFrame as a global temporary view
    val tempTableName = "products"
    productDF.createGlobalTempView(tempTableName)
    val globalTempViewName = s"global_temp.$tempTableName"

    //import apache spark sql
    import org.apache.spark.sql._

    //The following answers PART ONE questions of the problem scenario.
    //1. Select all the records with quantity >= 5000 and name starts with 'Pen'
    println("SELECTING: all the records with quantity >= 5000 and name starts with 'Pen'")
    val results1 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE quantity >= 5000 AND name LIKE 'Pen%'")
    println("SHOWING: all the records with quantity >= 5000 and name starts with 'Pen'")
    results1.show()

    //2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
    println("SELECTING: all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'")
    val results2 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE quantity >= 5000 AND price < 1.24 AND name LIKE 'Pen%'")
    println("SHOWING: all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'")
    results2.show()

    //3. Select all the records witch does not have quantity 5000 and name does not starts with 'Pen'
    println("SELECTING: all the records witch does not have quantity 5000 and name does not starts with 'Pen'")
    val results3 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE NOT (quantity >= 5000 AND name LIKE 'Pen%')")
    println("SHOWING: all the records witch does not have quantity 5000 and name does not starts with 'Pen'")
    results3.show()

    //4. Select all the products which name is 'Pen Red', 'Pen Black'
    println("SELECTING: all the products which name is 'Pen Red', 'Pen Black'")
    val results4 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE name IN ('Pen Red', 'Pen Black')")
    println("SHOWING: all the products which name is 'Pen Red', 'Pen Black'")
    results4.show()

    //5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000
    println("SELECTING : all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000")
    val results5 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE (price BETWEEN 1 AND 2) AND (quantity BETWEEN 1000 AND 2000)")
    println("SHOWING: all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000")
    results5.show()

    //Select all the products which has product code as null
    println("SELECTING : all the products which has product code as null")
    val results6 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE productCode IS NULL")
    println("SHOWING: all the products which has product code as null")
    results6.show()

    //Select all the products, whose name starts with Pen and results should be order by Price descending order.
    println("SELECTING : all the products, whose name stalls with Pen and results should be order by Price descending order.")
    val results7 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE name LIKE 'Pen%' ORDER BY price DESC")
    println("SHOWING: all the products, whose name startss with Pen and results should be order by Price descending order.")
    results7.show()

    //Select all the products, whose name staffs with Pen and results should be order by Price descending order and quantity ascending order.
    println("SELECTING : all the products, whose name staffs with Pen and results should be order by Price descending order and quantity ascending order")
    val results8 = spark.sql(s"SELECT * FROM $globalTempViewName WHERE name LIKE 'Pen%' ORDER BY price DESC, quantity ASC")
    println("SHOWING: all the products, whose name staffs with Pen and results should be order by Price descending order and quantity ascending order")
    results8.show()

    //Select top 2 products by price
    println("SELECTING : top 2 products by price")
    val results9 = spark.sql(s"SELECT * FROM $globalTempViewName ORDER BY price DESC LIMIT 2")
    println("SHOWING: top 2 products by price")
    results9.show()

    //Select all the columns from product table with output header as below.: `productID AS ID code AS Code name AS Description price AS 'Unit Price'
    println("SELECTING : all the columns from product table with output header as below.: `productID AS ID code AS Code name AS Description price AS 'Unit Price'")
    val results10 = spark.sql(s"SELECT productID AS ID, productCode AS Code, name AS Description, price AS Unit_Price FROM $globalTempViewName")
    println("SHOWING: all the columns from product table with output header as below.: `productID AS ID code AS Code name AS Description price AS 'Unit Pric'")
    results10.show()

    //Select code and name both separated by - and header name should be ProductDescription'
    println("SELECTING : code and name both separated by ' and header name should be ProductDescription'")
    val results11 = spark.sql(s"SELECT CONCAT(productID,'-',name) AS ProductDescription FROM $globalTempViewName")
    println("SHOWING: code and name both separated by ' and header name should be ProductDescription'")
    results11.show()

    //Select all distinct prices
    println("SELECTING : all distinct prices")
    val results12 = spark.sql(s"SELECT DISTINCT price AS Distinct_Price FROM $globalTempViewName")
    println("SHOWING: all distinct prices")
    results12.show()

    //Select distinct price and name combination
    println("SELECTING : distinct price and name combination")
    val results13 = spark.sql(s"SELECT DISTINCT price, name FROM $globalTempViewName")
    println("SHOWING: distinct price and name combination")
    results13.show()

    //Select all price data sorted by both code and productID combination
    println("SELECTING : all price data sorted by both code and productID combinatiom")
    val results15 = spark.sql(s"SELECT * FROM $globalTempViewName ORDER BY productID, productID")
    println("SHOWING: all price data sorted by both code and productID combinatiom")
    results15.show()


    //Count number ofproducts for each code
    println("SELECTING : Count number of products for each code")
    val results16 = spark.sql(s"SELECT * FROM $globalTempViewName ORDER BY price DESC LIMIT 2")
    println("SHOWING: Count number ofproducts for each code")
    results16.show()

    //save daraframe to hive table in orc format
    writeDataFrameToHiveTable(productDF, SaveMode.Overwrite, "orc", "product_orc_table")

    //save daraframe to hive table in orc format
    writeDataFrameToHiveTable(productDF, SaveMode.Overwrite, "parquet", "product_parquet_table")

  }

  def writeDataFrameToHiveTable(inputDF: DataFrame, saveMode: SaveMode, dataFormat: String, hiveTableName: String) = {
    println(s"Starting to write dataframe to hive table with the following data format $dataFormat and hive table name: $hiveTableName")
    //match cases: json, parquet, jdbc, orc, libsvm, csv, text
    dataFormat match {
      case "json" => inputDF.write.mode(saveMode).format("json").saveAsTable(hiveTableName)
      case "parquet" => inputDF.write.mode(saveMode).format("parquet").saveAsTable(hiveTableName)
      case "jdbc" => inputDF.write.mode(saveMode).format("jdbc").saveAsTable(hiveTableName)
      case "orc" => inputDF.write.mode(saveMode).format("orc").saveAsTable(hiveTableName)
      case "csv" => inputDF.write.mode(saveMode).format("libsvm").saveAsTable(hiveTableName)
      case "text" => inputDF.write.mode(saveMode).format("text").saveAsTable(hiveTableName)
      case "libsvm" => inputDF.write.mode(saveMode).format("libsvm").saveAsTable(hiveTableName)
      case _ => "Invalid dataFormat. Allowed formats are: json, parquet, jdbc, orc, csv, text or libsvm" // the default, catch-all
    }

    println(s"End write dataframe to hive table with the following table name $dataFormat and hive table name: $hiveTableName")

  }


}
