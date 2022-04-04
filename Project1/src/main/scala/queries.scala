import org.apache.spark.sql.SparkSession
import scala.io.StdIn

object queries extends App {
  def queryHighestPeak(spark: SparkSession) : Unit = {
    println("Highest player count:")
    spark.sql("select name as Game, peak_players as Players, month_year from steamData " +
      "where peak_players = (select(max(peak_players)) from steamData)").show()
  }
  def queryLowestPeak(spark: SparkSession) : Unit = {
    println("Lowest player count:")
    spark.sql("select distinct name as Game, peak_players as Players, month_year from steamData " +
      "where peak_players = (select(min(peak_players)) from steamData where peak_players > 10000)").show(1)
  }
  def queryCurrentHighest(spark: SparkSession) : Unit = {
    println("Games with latest highest player count (September 2021):")
    spark.sql("select name as Game, peak_players as Players from steamData " +
      "where month_year = 'September 2021' " +
      "order by peak_players desc").show(10, false)
  }
  def queryHighestInX(spark: SparkSession) : Unit = {
    println(s"Please enter a valid month and year (Ex. \'September 2021\'): ")
    val input = StdIn.readLine()
    println(s"\nGames with highest player count in $input:")
    spark.sql("select name as Game, peak_players as Players, month_year as Date from steamData " +
      s"where month_year = '$input' order by peak_players desc").show(10, false)
  }
  def queryHighestPlayerMonth(spark: SparkSession) : Unit = {
    println("Dates with the highest total player count across all games:")
    spark.sql("select month_year as Date, sum(peak_players) as Total_Players from steamData group by month_year order by Total_Players desc").show(10, false)
  }
  def queryTopAverageInGame(spark: SparkSession) : Unit = {
    println(s"Please enter a valid game name (Ex. \'Destiny 2\'): ")
    val input = StdIn.readLine()
    println(s"Highest Average Player Count for $input:")
    spark.sql("select ceil(Avg_players) as Average, month_year as Date from steamData " +
      s"where name = '$input' order by Avg_players desc").show(10, false)
  }
}
