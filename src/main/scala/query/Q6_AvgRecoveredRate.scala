package query

import database.Resources.covid_19_data_clean
import database.SparkConnection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q6_AvgRecoveredRate extends App{
  def queryAvgRecoveredRate(spark: SparkSession) {
    val df = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).load(covid_19_data_clean).toDF()
    println("What is the average recovered rate(per day) by countries?")

    df.select(col("Recovered"), col("Country/Region"),col("ObservationDate"))
      .groupBy("Country/Region")
      .agg(round(max("Recovered") / countDistinct("ObservationDate")).as("Avg_Recovered_Rate"))
      .sort(desc("Avg_Recovered_Rate"))
      .show(10, false)
  }
}