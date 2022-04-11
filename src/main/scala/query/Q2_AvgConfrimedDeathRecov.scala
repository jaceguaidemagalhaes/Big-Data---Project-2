package query

import database.Resources.covid_19_data_clean
import database.SparkConnection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q2_AvgConfrimedDeathRecov extends App{
  def queryAvgConDeathRecov(spark: SparkSession) {
    val df = spark.read.format("csv").option("header", true).load(covid_19_data_clean).toDF()

    println("Average confirmed, deaths, and recovers:")
    val avgCovidData = df.filter(df("ObservationDate") === "2021-05-02")
      .agg(round(sum("Confirmed") / 487).as("Average_Confirmed"),
      round(sum("Deaths") / 487).as("Average_Deaths"),
      round(sum("Recovered") / 487).as("Average_Recovered"))
    avgCovidData.show
  }
}