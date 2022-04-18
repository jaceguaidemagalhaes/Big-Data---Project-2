package query

import database.Resources._
import database.SparkConnection
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.functions.{col, sum}
import scala.io.StdIn.readLine

object Q8_Total_CDR extends App {
  // Total confirmed, death, and recover
  def queryTotalCDR(spark: SparkSession): Unit = {
    val df_covid_19: DataFrame = spark.read.options(Map("header"->"true", "inferSchema"->"true", "delimiter"->",")).csv(covid_19_data_clean).na.fill(0).na.fill("")
    // To Determine the last day in the data set:
    //val lastDay = df_covid_19.select(last("ObservationDate")).show()

    val dfTotalCDR = df_covid_19.select(col("Confirmed").cast(IntegerType), col("Deaths").cast(IntegerType), col("Recovered").cast(IntegerType))
      .where(df_covid_19("ObservationDate") === "2021-05-02")
      .agg(sum(col("Confirmed").cast(IntegerType)).as("TotalConfirmed"), sum(col("Deaths").cast(IntegerType)).as("TotalDeaths"), sum(col("Recovered").cast(IntegerType)).as("TotalRecovered"))

    dfTotalCDR.show(false)

    Thread.sleep(1000)

    dfTotalCDR.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv(workingPath+"results/q8_TotalCDR")

    readLine("Enter anything to continue: ")

  }
}