# big-data-insights-scala
personal solutions to big data problem scenarios using scala 

## Problem Scenarios

### 1. [Product Data for a pen company](https://github.com/jwkimani/big-data-insights-scala/blob/master/problem_scenarios/scala_problem_scenario_1-products.md) 
   
### 2. [Patient Data]()

### 3. [RDD (resilient distributed dataset) Operations]()


## Troubleshooting
1. When running applications if below error occurs: *A master URL must be set in your configuration*
    ```
    Exception in thread "main" java.lang.ExceptionInInitializerError
        at com.jwk.development.big_data_insights.scala.products.driver.problem_scenario_1.main(problem_scenario_1.scala)
    Caused by: org.apache.spark.SparkException: A master URL must be set in your configuration
    ```
    
    Solution: 
    
    Add the following VM option to your run configurations
    ```
    -Dspark.master=local    
    ```
    [How to set spark master to local in intellij](https://github.com/jwkimani/big-data-insights-scala/blob/master/wiki_data/screenshots/how_to_set_spark_master_to_local_in%20_intellij.PNG)
   
