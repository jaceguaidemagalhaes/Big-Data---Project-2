package database

import org.apache.spark.sql.SparkSession

import scala.io.StdIn._


object queries extends App {
  //These are all example queries for our Project. We may change them as we see fit when covid table is finished and cleaned up
  //1. What are the top 10 countries with greatest amount of deaths?
  def queryHighestPeak(spark: SparkSession): Unit = {
    println("Top 10 countries with greatest amount of deaths:")
    spark.sql("SELECT Country, Death FROM table_name ORDER BY Death DESC").show(10, false)
  }
  //2. Select percentage of population confirmed, dead, and recovered

  //3. Cumulative average confirmed, deaths, and recovers
  def queryAvg(spark: SparkSession): Unit = {
    println("Average confirmed, deaths, and recovers:")
    spark.sql("SELECT ROUND(AVG(confirmed),0), ROUND(AVG(Deaths),0), ROUND(AVG(Recovers),0) FROM table_name").show(10, false)
  }
  //4. When was the peak of mortality rate of the pandemic?
  def queryHighestDeath(spark: SparkSession): Unit = {
    println("When was the peak of mortality rate of the pandemic?")
    spark.sql("SELECT Date, MAX(Death) as Mortality rate from table_name ORDER BY Date").show(10, false)
  }
  //5. What are the correlation between deaths and population?

  //6. What is the average recovered rate by countries?
  def queryAverageRecoveredRate(spark: SparkSession): Unit = {
    println("What is the average recovered rate by countries?")
    spark.sql("SELECT Country, ROUND(AVG(Recovered),0) AS Recovered rate FROM table_name").show(10, false)
  }
  //7. Confirmed by day, country, and states (when applicable)

  //8. Total confirmed, death, and recover

  //9. Confirmed spread speed for determined time and country
  def queryConSpreadSpeed(spark: SparkSession): Unit = {
    println("Confirmed spread speed for determined time and country")
    val country = readLine("Type in the country: ")
    // the dates can be changed according to the table
    val startDate = readLine("Start date (yyyy/mm/dd): ")
    val endDate = readLine("End date (yyyy/mm/dd): ")
    spark.sql(s"SELECT Country, ROUND(AVG(confirmed),0) AS Avg Confirmed Per Day FROM table_name " +
      s"WHERE (Date BETWEEN $startDate AND $endDate) AND (Country = '$country')").show(10,false)
  }
  //10. Death spread speed for determined time and country
  def queryDeathSpreadSpeed(spark: SparkSession): Unit = {
    println("Death spread speed for determined time and country")
    val country = readLine("Type in the country: ")
    // the dates can be changed according to the table
    val startDate = readLine("Start date (yyyy/mm/dd): ")
    val endDate = readLine("End date (yyyy/mm/dd): ")
    spark.sql(s"SELECT Country, ROUND(AVG(Death),0) AS Avg Death Per Day FROM table_name " +
      s"WHERE (Date BETWEEN $startDate AND $endDate) AND (Country = '$country')").show(10, false)
  }
}