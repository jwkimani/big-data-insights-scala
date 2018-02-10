#!/usr/bin/env bash

hive -e
"
CREATE EXTERNAL TABLE products_orc (productid int, code string, name string, quantity int, price float)
STORED AS orc
LOCATION /user/hive/warehouse/product_orc_table
;"

#select * from product_orc_table