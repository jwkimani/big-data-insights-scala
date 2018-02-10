# Problem Scenario 1
This problem has three parts:
##
1. [Part One](#part1)
   
2. [Part Two](#part2)
   
3. [Part Three](#part3)

## Part One <a name="part1"></a>

You have the following tab delimited csv file [products.csv](https://github.com/jwkimani/big-data-insights-scala/blob/master/insight_data/products.csv)

``` 
products.csv

+---------+-----------+---------+--------+-------+
|productID|productCode|     name|quantity|  price|
+---------+-----------+---------+--------+-------+
|     1001|        PEN|  Pen Red|    5000|   1.23|
|     1002|        PEN| Pen Blue|    8001|   1.25|
|     1003|        PEN|Pen Black|    2000|   1.25|
|     1004|        PEC|Pencil 2B|   10000|   0.48|
|     1005|        PEC|Pencil 2H|    8000|   0.49|
|     1006|        PEC|Pencil HB|       0|9999.99|
|     2001|        PEC|Pencil 3B|     500|   0.52|
|     2002|        PEC|Pencil 4B|     200|   0.62|
|     2003|        PEC|Pencil 5B|     100|   0.73|
|     2004|        PEC|Pencil 6B|     500|   0.47|
+---------+-----------+---------+--------+-------+
```
Using Spark and SparkSQL perform the following tasks: 
1. Load the csv file to a dataframe 
    
    Using schema: 
    ```
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
    ```
    approach
    ```
    val productDF = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","true").option("inferSchema", "false").schema(schema).load(path)
    ```

2. Create a global temporary view named `products` from dataframe with data

    `productDF.createGlobalTempView("products")`

3. Using the global temporary view, perform the task below

4. Select and show all the records with quantity >= 5000 and name starts with 'Pen' 
    ``` 
    +---------+-----------+---------+--------+-----+
    |productID|productCode|     name|quantity|price|
    +---------+-----------+---------+--------+-----+
    |     1001|        PEN|  Pen Red|    5000| 1.23|
    |     1002|        PEN| Pen Blue|    8001| 1.25|
    |     1004|        PEC|Pencil 2B|   10000| 0.48|
    |     1005|        PEC|Pencil 2H|    8000| 0.49|
    +---------+-----------+---------+--------+-----+
    ```

5. Select and show all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen' 
    ``` 
    +---------+-----------+---------+--------+-----+
    |productID|productCode|     name|quantity|price|
    +---------+-----------+---------+--------+-----+
    |     1001|        PEN|  Pen Red|    5000| 1.23|
    |     1004|        PEC|Pencil 2B|   10000| 0.48|
    |     1005|        PEC|Pencil 2H|    8000| 0.49|
    +---------+-----------+---------+--------+-----+
    ```

6. Select and show all the records witch does not have quantity 5000 and name does not starts with 'Pen' 
    ``` 
    +---------+-----------+---------+--------+-------+
    |productID|productCode|     name|quantity|  price|
    +---------+-----------+---------+--------+-------+
    |     1003|        PEN|Pen Black|    2000|   1.25|
    |     1006|        PEC|Pencil HB|       0|9999.99|
    |     2001|        PEC|Pencil 3B|     500|   0.52|
    |     2002|        PEC|Pencil 4B|     200|   0.62|
    |     2003|        PEC|Pencil 5B|     100|   0.73|
    |     2004|        PEC|Pencil 6B|     500|   0.47|
    +---------+-----------+---------+--------+-------+
    ```

7. Select and show all the products which name is 'Pen Red', 'Pen Black' 
    ``` 
    +---------+-----------+---------+--------+-----+
    |productID|productCode|     name|quantity|price|
    +---------+-----------+---------+--------+-----+
    |     1001|        PEN|  Pen Red|    5000| 1.23|
    |     1003|        PEN|Pen Black|    2000| 1.25|
    +---------+-----------+---------+--------+-----+
    ```
8. Select and show all the products which has price BETWEEN 1.0 AND 2 0 AND quantity 
    ``` 
    +---------+-----------+---------+--------+-----+
    |productID|productCode|     name|quantity|price|
    +---------+-----------+---------+--------+-----+
    |     1003|        PEN|Pen Black|    2000| 1.25|
    +---------+-----------+---------+--------+-----+
    ```
    <!--question 88-->
9. Select all the products which has product code as null 

    ``` 
    +---------+-----------+----+--------+-----+
    |productID|productCode|name|quantity|price|
    +---------+-----------+----+--------+-----+
    +---------+-----------+----+--------+-----+
    ```

10. Select all the products, whose name stalls with Pen and results should be order by Price descending order. 

    ``` 
    +---------+-----------+---------+--------+-------+
    |productID|productCode|     name|quantity|  price|
    +---------+-----------+---------+--------+-------+
    |     1006|        PEC|Pencil HB|       0|9999.99|
    |     1003|        PEN|Pen Black|    2000|   1.25|
    |     1002|        PEN| Pen Blue|    8001|   1.25|
    |     1001|        PEN|  Pen Red|    5000|   1.23|
    |     2003|        PEC|Pencil 5B|     100|   0.73|
    |     2002|        PEC|Pencil 4B|     200|   0.62|
    |     2001|        PEC|Pencil 3B|     500|   0.52|
    |     1005|        PEC|Pencil 2H|    8000|   0.49|
    |     1004|        PEC|Pencil 2B|   10000|   0.48|
    |     2004|        PEC|Pencil 6B|     500|   0.47|
    +---------+-----------+---------+--------+-------+
    ```

11. Select all the products, whose name staffs with Pen and results should be order by Price descending order and quantity ascending order. 

    ``` 
    +---------+-----------+---------+--------+-------+
    |productID|productCode|     name|quantity|  price|
    +---------+-----------+---------+--------+-------+
    |     1006|        PEC|Pencil HB|       0|9999.99|
    |     1003|        PEN|Pen Black|    2000|   1.25|
    |     1002|        PEN| Pen Blue|    8001|   1.25|
    |     1001|        PEN|  Pen Red|    5000|   1.23|
    |     2003|        PEC|Pencil 5B|     100|   0.73|
    |     2002|        PEC|Pencil 4B|     200|   0.62|
    |     2001|        PEC|Pencil 3B|     500|   0.52|
    |     1005|        PEC|Pencil 2H|    8000|   0.49|
    |     1004|        PEC|Pencil 2B|   10000|   0.48|
    |     2004|        PEC|Pencil 6B|     500|   0.47|
    +---------+-----------+---------+--------+-------+
    ```

12. Select top 2 products by price 

    ``` 
    +---------+-----------+---------+--------+-------+
    |productID|productCode|     name|quantity|  price|
    +---------+-----------+---------+--------+-------+
    |     1006|        PEC|Pencil HB|       0|9999.99|
    |     1002|        PEN| Pen Blue|    8001|   1.25|
    +---------+-----------+---------+--------+-------+
    ```

    <!--question 21-->
13. Select all the columns from product table with output header as below.
    `
    productID AS ID
    code AS Code 
    name AS Description 
    price AS 'Unit Price'
    `
    ``` 
    
    ```

14. Select code and name both separated by '-' and header name should be ProductDescription'_ 

    ``` 
    ```

15. Select all distinct prices. 

    ``` 
    ```

16. Select distinct price and name combination 

    ``` 
    ```

17. Select all price data sorted by both code and productID combinatiom 

    ``` 
    ```

18. count number of products. 

    ``` 
    ```

19. Count number ofproducts for each code 

    ``` 
    ```

    <!--question 14-->
20. Select Maximum, minimum, average Standard Deviation, and total quantity _ 

21. Select minimum and maximum price for each product code. 

22. Select Maximum, minimum, average Standard Deviation, and total quantity for each product code, hwoeiM make sure and Standard deviation will have maximum two decimal values. 

23. Select all the product code and average price only where product count is more than or equal to 3 

24. Select maximum, minimum average and total of all the products for each code. Also produce the same across all the products 


## Part Two <a name="part2"></a>

You have been provided two additional files:

1. [suppliers.csv](https://github.com/jwkimani/big-data-insights-scala/blob/master/insight_data/supplier.csv)

2. [products_suppliers.csv](https://github.com/jwkimani/big-data-insights-scala/blob/master/insight_data/products_suppliers.csv)


Now accomplish all the queries. 
    <!--q70-->
1. Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL 

    <!--q37-->
2. It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to 
each supplier. 

3. Find all the supllier name, who are supplying 'Pencil 3B' 

4. Find all the products , which are supplied by ABC Traders _ 


## Part Three <a name="part3"></a>
1. Create a Hive ORC table using SparkSQL

2. Load this data in Hive table.

3. Create a Hive parquet table using SparkSQL and load data in it.


## Developer Notes:
Add the following VM options arguments to set spark master 
```
-Dspark.master=local
-Dhadoop.home.dir=C:\hadoop-2.7.4
```
