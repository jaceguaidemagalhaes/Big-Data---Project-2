package query

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
  }
