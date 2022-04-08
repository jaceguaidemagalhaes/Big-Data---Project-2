package query

import database.Resources._
import database.SparkConnection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Q2_HighestDeathByCountry extends App{
  def queryHighestDeath(spark: SparkSession): Unit = {
    println("Top 10 countries with greatest amount of deaths:")
  val df = spark.read.format("csv").option("header", true).load(covid_19_data_clean).toDF()

  val dfDeaths = df.select(col("Deaths").cast(IntegerType),col("Country/Region"))
    .groupBy("Country/Region").agg(max("Deaths").as("Deaths")).sort(desc("Deaths"))
    dfDeaths.show(10)
}
}
