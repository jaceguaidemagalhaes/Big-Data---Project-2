package query
import database.Resources._
import org.apache.avro.generic.GenericData.StringType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, lit, regexp_replace}
import org.apache.spark.sql.types.{DataType, IntegerType, Metadata, StringType, StructField, StructType}
import ui._
import org.apache.spark.sql.functions.format_number


import scala.reflect.io.File

object Q1_PercentageOfPopConfirmed extends App {
//class Q1_PercentageOfPopConfirmed{
  //query to have the percentage of population confirmed, recovered and death with covid
  // for each country
  //trello 17
  //jaceguai 4/06/2021 11:14PM Est

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

  executeQuery(spark)
  //val spark = ui.main.spark
  //val sc = spark.sparkContext
  def executeQuery(spark: SparkSession):Unit={
    //read covid data from csv file
    val covid_19_data = spark.read.format("csv").option("header", true).load(covid_19_data_clean).toDF()
    //read populatin by country for year 2020 and 2021. We will filter only to 2021 ahead
    val dftotalPopByCountry = spark.read.format("csv").option("header", false).load(totalPopByCountry).toDF("Country", "Population", "Year")
    //get total of confirmed cases by country for last update
    val df_TotalConfirmed = covid_19_data.select(col("Country/Region").alias("tcCountry"), col("Confirmed").cast(IntegerType))
    .groupBy("tcCountry").max("Confirmed")
    //get total recovered by country for last update
    val df_TotalRecovered = covid_19_data.select(col("Country/Region").alias("trCountry"),
    col("Recovered").cast(IntegerType))
    .groupBy("trCountry").max("Recovered")
    //get total deaths by country for last update
    val df_TotalDeaths = covid_19_data.select(col("Country/Region").alias("tdCountry"),
    col("Deaths").cast(IntegerType))
    .groupBy("tdCountry").max("Deaths")
    //join above dataframe
    val df_popConf = dftotalPopByCountry.join(df_TotalConfirmed, dftotalPopByCountry("Country")===df_TotalConfirmed("tcCountry"), "inner")
    .join(df_TotalRecovered, dftotalPopByCountry("Country")===df_TotalRecovered("trCountry"), "inner")
    .join(df_TotalDeaths, dftotalPopByCountry("Country")===df_TotalDeaths("tdCountry"),"inner")
    .filter(dftotalPopByCountry("Year")==="2021").toDF()

    //create, format, and save csv file

    val df_percentPopByCountry = df_popConf.select(col("Country"),
    col("Population").cast(IntegerType),
    (col("max(Confirmed)")/col("Population")).alias("Confirmed"),
    (col("max(Recovered)")/col("Population")).alias("Recovered"),
    (col("max(Deaths)")/col("Population")).alias("Deaths"))

    val percentPopByCountry = df_percentPopByCountry.withColumn("Confirmed", format_number(df_percentPopByCountry("Confirmed"), 5))
    .withColumn("Recovered", format_number(df_percentPopByCountry("Recovered"), 5))
    .withColumn("Deaths", format_number(df_percentPopByCountry("Deaths"), 5))
    percentPopByCountry.show()
    percentPopByCountry.coalesce(1).write.mode(SaveMode.Overwrite).csv(workingPath+"results/percentPopByCountry")
  }



  //end Q1_PercentageOfPopConfirmed
  }
