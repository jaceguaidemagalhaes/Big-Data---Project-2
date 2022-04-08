package query

import database.Resources.covid_19_data_clean
import database.SparkConnection
import org.apache.spark.sql.SparkSession

object Q7_PeakOfDeaths {
def queryMortalityRate(spark:SparkSession): Unit ={

}
  val spark = SparkConnection.sparkConnect()
  val df = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).load(covid_19_data_clean).toDF()
}
