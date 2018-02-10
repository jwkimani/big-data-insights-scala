package com.jwk.development.big_data_insights.scala.products.driver

import java.util.Date

import com.jwk.development.big_data_insights.scala.products.problem_scenario.part_One
import org.apache.spark.sql.SparkSession

object run_problem_scenario_part_One {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  def main(args: Array[String]): Unit = {

    //signal start message
    println("Start " + this.getClass.getName() + " : " + new Date())

    try {
      val problemPart = new part_One
      problemPart.part_One_Solution("insight_data/products.csv")
    } catch {
      case ex: Exception => {
        println(this.getClass.getName() + ". Error during program run. Root cause: " + ex.getMessage())
      }
    }

    //signal end message
    println("End " + this.getClass.getName() + " : " + new Date())
  }
}
