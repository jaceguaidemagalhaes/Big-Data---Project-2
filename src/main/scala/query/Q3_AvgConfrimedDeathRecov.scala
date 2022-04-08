package query

import database.Resources.covid_19_data_clean
import database.SparkConnection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q3_AvgConfrimedDeathRecov extends App{
  def queryAvgConDeathRecov(spark: SparkSession) {
    val df = spark.read.format("csv").option("header", true).load(covid_19_data_clean).toDF()

    println("Average confirmed, deaths, and recovers:")
    val avgCovidData = df.agg(round(max("Confirmed") / countDistinct("ObservationDate")).as("Average_Confirmed"),
      round(max("Deaths") / countDistinct("ObservationDate")).as("Average_Deaths"),
      round(max("Recovered") / countDistinct("ObservationDate")).as("Average_Recovered"))
    avgCovidData.show
  }
}