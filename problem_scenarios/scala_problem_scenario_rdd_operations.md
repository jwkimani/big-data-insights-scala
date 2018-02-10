#Problem Scenario: RDD join

You have been given below code snippet 

```
val a = "salmon", "salmon", "rat", "elephant"}, 3} 

val b = a.keyBy(_.1ength) 

val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", )), "salmon" , 3) 

val d = c.keyBy(_.1ength) 

``` 

Write a correct code snippet for operationl which will produce desired output, shoun below. 

```
Array[(lnt, (String, String))] = 

(6 , (salmon,rabbit)) 

(3,(dog,dog)), (3 ,(dog,cat)), (3 ,(dog,gnu)),
```

