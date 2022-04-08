package query
<<<<<<< Updated upstream

object Q1_PercentageOfPopConfirmed {


  import org.apache.avro.generic.GenericData.StringType
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
  import org.apache.spark.sql.functions.{col, lit, regexp_replace}
  import org.apache.spark.sql.types.{DataType, IntegerType, Metadata, StringType, StructField, StructType}



    //query to have the percentage
    //it's only to use for data manipulation purposes
    //jaceguai 4/06/2021 3:55 Est


    System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")
    println("Creating Spark session....")
    Logger.getLogger("org").setLevel(Level.ERROR)//remove messages
    val spark = SparkSession
      .builder
      .appName("AnalyseThat Hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")//remove messages
    println("created spark session")

    var path = System.getProperty("user.dir")+"/rawdata/"

    val df_pop2021 = spark.read.option("header", true).csv(path+"2021_population.csv")
    //df_pop2021.printSchema()
    //df_pop2021.show()

    val df_covid_19_data = spark.read.option("header", true).csv(path+"covid_19_data.csv")
    //df_covid_19_data.printSchema()
    //df_covid_19_data.show()

    val df_pop2020 = spark.read.option("header", true).csv(path+"population_by_country_2020.csv")
    //df_pop2020.printSchema()
    //df_pop2020.show()

    //year = 2020
    val df_pop2020col = df_pop2020.select(col("Country (or dependency)").alias("country"),
      col("Population (2020)").alias("population"), lit(2020).as("Year"))
    //df_pop2020col.show()

    //year = 2021
    val df_pop2021colcomma = df_pop2021.select(col("country").alias("country"),
      col("2021_last_updated").alias("population"), lit(2021).as("Year"))
    df_pop2021colcomma.show()

    val df_pop2001col = df_pop2021colcomma.withColumn("population",
      regexp_replace(df_pop2021colcomma("population"), ",", ""))
    //df_pop2001col.show()

    val df_totalPop2020_2021 = df_pop2020col.union(df_pop2001col)
    df_totalPop2020_2021.show()
    df_totalPop2020_2021.coalesce(1).write.csv(path+"totalByCountry")

    //val df_join = df_totalPop2020_2021.join(df_covid_19_data,df_totalPop2020_2021("country") ===  df_covid_19_data("Country/Region"),"outer")
    //df_join.printSchema()
    //  df_join.show(false)
    //df_join.coalesce(1).write.csv(path+"compwithcoviddata")

    //val df_nullcountry = df_join.filter(df_join("country").isNull
    //    || df_join("Country/Region").isNull)
    //    df_nullcountry.show()
    //df_nullcountry.coalesce(1).write.csv(path+"nullcountries.csv")

    //val df_distinct = df_nullcountry.select(col("country"),col("Country/Region")).distinct()
    //df_distinct.count()
    //df_distinct.coalesce(1).write.csv(path+"nullcountries.csv")

    //    val df_join = df_pop2020col.join(df_pop2021col,df_pop2020col("country") ===  df_pop2021col("country"),"outer")
    //      .toDF("country2020", "population2020","year2020","country2021", "population2021", "year2021")
    //      df_join.show(false)

    //    val df_nullcountry = df_join.filter(df_join("country2020").isNull
    //        || df_join("country2021").isNull)
    //        df_nullcountry.show()
    //    df_nullcountry.coalesce(1).write.csv(path+"nullcountries.csv")

    //    case class StructType(fields: Array[StructField])
    //    case class StructField(
    //                            name: String,
    //                            dataType: DataType,
    //                            nullable: Boolean = true,
    //                            metadata: Metadata = Metadata.empty)
    //
    //    val simpleSchema = StructType(Array(
    //        StructField("country2020", org.apache.spark.sql.types.StringType,true),
    //        StructField("population2020",org.apache.spark.sql.types.StringType,true),
    //        StructField("country2021",org.apache.spark.sql.types.StringType,true),
    //        StructField("population2021", org.apache.spark.sql.types.StringType, true),
    //        ))

    //val df_join1 = df_join.select("*").toDF("country2020", "population2020","country2021", "population2021")

    //end preparedata
=======
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

//object Q1_PercentageOfPopConfirmed extends App {
class Q1_PercentageOfPopConfirmed{
  //query to have the percentage of population confirmed, recovered and death with covid
  // for each country
  //trello 17
  //jaceguai 4/06/2021 11:14PM Est

//  System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")
//  println("Creating Spark session....")
//  Logger.getLogger("org").setLevel(Level.ERROR)//remove messages
//  val spark = SparkSession
//    .builder
//    .appName("Project2")
//    .config("spark.master", "local")
//    .enableHiveSupport()
//    .getOrCreate()
//  spark.sparkContext.setLogLevel("ERROR")//remove messages
//  println("created spark session")

  //executeQuery(spark)
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
    ((col("max(Confirmed)")/col("Population"))*100).alias("Confirmed"),
    ((col("max(Recovered)")/col("Population"))*100).alias("Recovered"),
    ((col("max(Deaths)")/col("Population"))*100).alias("Deaths"))

    val percentPopByCountry = df_percentPopByCountry.withColumn("Confirmed", format_number(df_percentPopByCountry("Confirmed"), 5))
    .withColumn("Recovered", format_number(df_percentPopByCountry("Recovered"), 5))
    .withColumn("Deaths", format_number(df_percentPopByCountry("Deaths"), 5))
    percentPopByCountry.show()
    percentPopByCountry.coalesce(1).write.mode(SaveMode.Overwrite).csv(workingPath+"results/percentPopByCountry")
  }



  //end Q1_PercentageOfPopConfirmed
>>>>>>> Stashed changes
  }
