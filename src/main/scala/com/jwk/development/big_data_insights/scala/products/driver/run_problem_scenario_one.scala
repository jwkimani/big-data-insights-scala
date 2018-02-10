package com.jwk.development.big_data_insights.scala.products.driver

import java.util.Date

import com.jwk.development.big_data_insights.scala.products.problem_scenario_One
import org.apache.spark.sql.SparkSession

object run_problem_scenario_one {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  def main(args: Array[String]): Unit = {

    println("Start " + this.getClass.getName() + " : " + new Date())
    try {
      val solution = new problem_scenario_One
      solution.problem_scenario1("insight_data/products.csv")
    } catch {
      case ex: Exception => {
        println(this.getClass.getName() + ". Error during transformation. Root cause: " + ex.getMessage())
      }
    }
    println("End " + this.getClass.getName() + " : " + new Date())
  }
}
