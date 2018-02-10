# big-data-insights-scala
personal solutions to big data problem scenarios using scala 

##Project Structure
Each package is based on a problem scenario.
Each problem scenario will contain a main class in the *com.jwk.development.big_data_insights.scala.products.driver* package
Each problem contains a problem scenario detail and result sheet.

###1. Product Data for a pen company 
    
    Problem: Given csv files with product information from a pen company, provide some insights using big data technologies
    
    Package name: *com.jwk.development.big_data_insights.scala.products.problem_scenario_One*
    
    Driver/Main class: *com.jwk.development.big_data_insights.scala.products.driver.run_problem_scenario_one*
   
    Link to result sheet and detailed problem scenarions:
    
     [Part One]()
     
     [Part Two]()
     
     [Part Three]()

###2. Patient Data

    Problem: **
        
    Package name: **
        
    Driver/Main class: **

    Link to result sheet and detailed problem scenarions:
        

##Troubleshooting
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
    [link to setting spark master to local in intellij]()
   
