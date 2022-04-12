package query

import database.Resources.{covid_19_data_clean, workingPath}
import database.SparkConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.StdIn._

object Q9_ConSpreadSpeed extends App{
  def queryConSpreadSpeed(spark:SparkSession) {
    val df = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).load(covid_19_data_clean).toDF()
    println("Confirmed spread speed for determined time and country")
    val country = readLine("Type in the country: ")
    val startDate = readLine("Start date. Earliest date recorded 2020-01-22 (yyyy-mm-dd): ")
    val endDate = readLine("End date. Most recent date recorded 2021-05-02 (yyyy-mm-dd): ")

    df.where(df("Country/Region").contains(s"$country") && df("ObservationDate").between(startDate, endDate))
      .groupBy("Country/Region")
      .agg(max("Confirmed").as("Total_Confrimed"),
        round(max("Confirmed") / countDistinct("ObservationDate"), 2).as("Average_Confirmed_per_day"))
      .show

    df.coalesce(1).write
        .mode(SaveMode.Overwrite)
        .option("header", true)
        .csv(workingPath+"results/q9_ConSpreadSpeed")
  }
  val spark = SparkConnection.sparkConnect()
  queryConSpreadSpeed(spark)
}