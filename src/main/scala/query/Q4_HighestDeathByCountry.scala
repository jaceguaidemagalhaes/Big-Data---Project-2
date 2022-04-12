package query

import database.Resources._
import database.SparkConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Q4_HighestDeathByCountry extends App{
  def queryHighestDeath(spark: SparkSession): Unit = {
    println("Top 10 countries with greatest amount of deaths:")
  val df = spark.read.format("csv").option("header", true).load(covid_19_data_clean).toDF()

  val dfDeaths = df.select(col("Deaths").cast(IntegerType),col("Country/Region"))
    .groupBy("Country/Region").agg(max("Deaths").as("Deaths")).sort(desc("Deaths"))
    dfDeaths.show(10)

    dfDeaths.coalesce(1).write
        .mode(SaveMode.Overwrite)
        .option("header", true)
        .csv(workingPath+"results/q4_Top10CountriesHighestDeaths")
}
}
