package query
import database.Resources.covid_19_data_clean
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.StdIn._

object Q6_DeathSpreadSpeed extends App{
  def queryDeathSpreadSpeed(spark:SparkSession) {
    val df = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).load(covid_19_data_clean).toDF()
    println("Confirmed spread speed for determined time and country")
    val country = readLine("Type in the country: ")
    val startDate = readLine("Start date. Earliest date recorded 2020-01-22 (yyyy-mm-dd): ")
    val endDate = readLine("End date. Most recent date recorded 2021-05-02 (yyyy-mm-dd): ")
    df.where(df("Country/Region").contains(s"$country") && df("ObservationDate").between(startDate, endDate))
      .groupBy("Country/Region")
      .agg(max("Deaths").as("Total_Deaths"),
        round(max("Deaths") / countDistinct("ObservationDate"), 2).as("Average_Deaths_per_day"))
      .show
  }
}