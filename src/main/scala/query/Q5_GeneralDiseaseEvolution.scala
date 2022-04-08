package query

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import database.Resources
import database.Resources.workingPath
import org.apache.spark.sql.functions.{col, column, date_add}
import org.apache.spark.sql.types.DateType

object Q5_GeneralDiseaseEvolution extends App{

    System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")
    println("Creating Spark session....")
    Logger.getLogger("org").setLevel(Level.ERROR)//remove messages
    val spark = SparkSession
      .builder
      .appName("Project2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")//remove messages
    println("created spark session")

  val df_covid_19_data: DataFrame = spark.read.options(Map("header"->"true", "inferSchema"->"true", "delimiter"->","))
    .csv(Resources.covid_19_data_clean)
  //df_covid_19_data.show()
  //df_covid_19_data.printSchema()

  val df_covid_19_data_confirmed = df_covid_19_data.withColumn("ObservationDate", col("ObservationDate").cast(DateType))
    .groupBy("Country/Region","ObservationDate")
    .sum("Confirmed")
    .cache()
    //.orderBy("Country/Region", "ObservationDate")
  //df_covid_19_data_confirmed.show()
  //df_covid_19_data_confirmed.printSchema()

  val df_covid_19_data_recovered = df_covid_19_data.withColumn("ObservationDate", col("ObservationDate").cast(DateType))
    .groupBy("Country/Region","ObservationDate")
    .sum("Recovered")
    .cache()
   // .orderBy("Country/Region", "ObservationDate")
  //df_covid_19_data_recovered.show()
  //df_covid_19_data_recovered.printSchema()

  val df_covid_19_data_deaths = df_covid_19_data.withColumn("ObservationDate", col("ObservationDate").cast(DateType))
    .groupBy("Country/Region","ObservationDate")
    .sum("Deaths")
    .cache()
    //.orderBy("Country/Region", "ObservationDate")
  //df_covid_19_data_deaths.show()
 // df_covid_19_data_deaths.printSchema()

  val df_dif_confirmed_temp = df_covid_19_data_confirmed.as("conf2").join(df_covid_19_data_confirmed.as("conf1"),
    col("conf1.Country/Region") === col("conf2.Country/Region")
      && col("conf1.ObservationDate") === date_add(col("conf2.ObservationDate"),1), "inner" )
    //.orderBy("conf2.Country/Region", "conf2.ObservationDate")
    .toDF("CountryConfirmed","InitialDateConf","InitialConfirmed", "CountryConf1", "FinalDateConf", "FinalConfirmed")
    .cache()

  val df_dif_confirmed = df_dif_confirmed_temp.select(col("CountryConfirmed"),
    col("FinalDateConf"),
    (col("FinalConfirmed")-col("InitialConfirmed")).alias("GrowthConfirmed"))
    .cache()

  //df_dif_confirmed.show()

  val df_dif_recovered_temp = df_covid_19_data_recovered.as("rec2").join(df_covid_19_data_recovered.as("rec1"),
    col("rec1.Country/Region") === col("rec2.Country/Region")
      && col("rec1.ObservationDate") === date_add(col("rec2.ObservationDate"),1), "inner" )
    //.orderBy("rec2.Country/Region", "rec2.ObservationDate")
    .toDF("CountryRecovered","InitialDateRec","InitialRec", "Countryrec1", "FinalDateRec", "FinalRec")
    .cache()

  val df_dif_recovered = df_dif_recovered_temp.select(col("CountryRecovered"),
    col("FinalDateRec"),
    (col("FinalRec")-col("InitialRec")).alias("GrowthRec"))
    .cache()
  //df_dif_recovered.show()

  val df_dif_deaths_temp = df_covid_19_data_deaths.as("deaths2").join(df_covid_19_data_deaths.as("deaths1"),
    col("deaths1.Country/Region") === col("deaths2.Country/Region")
      && col("deaths1.ObservationDate") === date_add(col("deaths2.ObservationDate"),1), "inner" )
    //.orderBy("deaths2.Country/Region", "deaths2.ObservationDate")
    .toDF("CountryDeaths","InitialDateDeaths","InitialDeaths", "CountryDeaths1", "FinalDateDeaths", "FinalDeaths")
    .cache()

  val df_dif_deaths = df_dif_deaths_temp.select(col("CountryDeaths"),
    col("FinalDateDeaths"),
    (col("FinalDeaths")-col("InitialDeaths")).alias("GrowthDeaths"))
    .cache()
  //df_dif_deaths.show()

  val df_confirmedcovidgrowthratio = df_covid_19_data_confirmed.select("Country/Region", "ObservationDate", "sum(Confirmed)").as("c19d")
    .join(df_dif_confirmed.as("dc"),
    col("c19d.Country/Region") === col("dc.CountryConfirmed") &&
    col("c19d.ObservationDate") === col("dc.FinalDateConf"), "inner")
    .cache()
  val df_recoveredcovidgrowthratio = df_covid_19_data_recovered.select("Country/Region", "ObservationDate", "sum(Recovered)").as("c19dr")
    .join(df_dif_recovered.as("dr"),
      col("c19dr.Country/Region") === col("dr.CountryRecovered") &&
        col("c19dr.ObservationDate") === col("dr.FinalDateRec"), "inner")
    .cache()
  val df_deathscovidgrowthratio = df_covid_19_data_deaths.select("Country/Region", "ObservationDate", "sum(Deaths)").as("c19dd")
    .join(df_dif_deaths.as("dd"),
      col("c19dd.Country/Region") === col("dd.CountryDeaths") &&
        col("c19dd.ObservationDate") === col("dd.FinalDateDeaths"), "inner")
    .cache()


  //df_deathscovidgrowthratio.printSchema()

  val df_covidgrowthratio = df_confirmedcovidgrowthratio.as("c")
    .join(df_recoveredcovidgrowthratio.as("r"),
    col("c.Country/Region") === col("r.Country/Region") &&
    col("c.ObservationDate") === col("r.ObservationDate"))
    .join(df_deathscovidgrowthratio.as("d"),
      col("c.Country/Region") === col("d.Country/Region") &&
        col("c.ObservationDate") === col("d.ObservationDate"))
    .toDF("Country", "Date", "Confirmed", "Country1", "Date1", "GrowthConfirmed",
      "Country2", "Date2", "Recovered", "Country3", "Date3", "GrowthRecovered",
      "Country4", "Date4", "Deaths", "Country5", "Date5", "GrowthDeaths")
    .cache()

  //df_covidgrowthratio.show()
  //df_covidgrowthratio.printSchema()

  val covidgrowthratio = df_covidgrowthratio.select(col("Country"),
    col("Date"), col("Confirmed"), col("GrowthConfirmed"),
    col( "Recovered"), col("GrowthRecovered"),
    col("Deaths"), col("GrowthDeaths"))
    .orderBy("Country", "Date")
  covidgrowthratio.show()
  covidgrowthratio.coalesce(1).write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .csv(workingPath+"results/q5_generaldiseaseevolution")
  //end Query5_GeneralDiseaseEvolution
}
