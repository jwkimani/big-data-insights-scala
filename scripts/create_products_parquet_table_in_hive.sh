#!/usr/bin/env bash

hive -e
"
CREATE EXTERNAL TABLE products_orc (productid int, code string, name string, quantity int, price float)
STORED AS parquet
LOCATION /user/hive/warehouse/product_parquet_table
;"

#"select * from product_parquet_table;"