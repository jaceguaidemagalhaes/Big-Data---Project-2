package query

import database.Resources._
import database.SparkConnection
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.StdIn.readLine

object Q7_ConfirmedByDay extends App {
  // Confirmed by day, country, and states (when applicable)
  def queryConfirmedByDay(spark: SparkSession): Unit = {
    val df_confirmed: DataFrame = spark.read.options(Map("header"->"true", "inferSchema"->"true", "delimiter"->",")).csv(ts_confirmed_byCountry).na.fill(0).na.fill("")
    val df_confirmed_US: DataFrame = spark.read.options(Map("header"->"true", "inferSchema"->"true", "delimiter"->",")).csv(ts_confirmed_US_byState).na.fill(0).na.fill("")


    df_confirmed.select("*").show(Integer.MAX_VALUE, false)
    Thread.sleep(500)
    readLine("Enter anything to display next table: ")
    Thread.sleep(1000)
    df_confirmed_US.select("*").show(Integer.MAX_VALUE,false)
    Thread.sleep(500)
    readLine("Enter anything to continue: ")
    Thread.sleep(1000)

    df_confirmed.coalesce(1).write
        .mode(SaveMode.Overwrite)
        .option("header", true)
        .csv(workingPath+"results/q7_ConfirmedByDay")
  }
}