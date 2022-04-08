package query

import database.Resources.covid_19_data_clean
import database.SparkConnection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._

object Q3_PeakOfDeaths extends App{
def queryMortalityRate(spark:SparkSession): Unit ={

}
  val spark = SparkConnection.sparkConnect()
//  println("When was the peak of mortality rate of the pandemic?")

  val df = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).load(covid_19_data_clean).toDF()
  val dfFebruary2020 = df.filter(df("ObservationDate") === "2020-02-29")
                      .agg(sum("Deaths").as("Total_Deaths"))

val dfMarch2020 = df.filter(df("ObservationDate") === "2020-03-31")
                  .agg(sum("Deaths").as("Total_Deaths"))


}
