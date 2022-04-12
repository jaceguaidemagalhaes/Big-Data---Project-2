package query

import database.Resources.{covid_19_data_clean, workingPath}
import database.SparkConnection
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._

object Q3_PeakOfDeaths extends App {
  def queryMortalityRate(spark: SparkSession): Unit = {


    val spark = SparkConnection.sparkConnect()
    //  println("When was the peak of mortality rate of the pandemic?")

    val df = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).load(covid_19_data_clean).toDF()

    val dfJanuary2020 = df.filter(df("ObservationDate") === "2020-01-31")
      .agg(sum("Deaths").as("Total_January"))

    val dfFebruary2020 = df.filter(df("ObservationDate") === "2020-02-29")
      .agg(sum("Deaths").as("Total_February"))

    val dfMarch2020 = df.filter(df("ObservationDate") === "2020-03-31")
      .agg(sum("Deaths").as("Total_March"))

    val dfApril2020 = df.filter(df("ObservationDate") === "2020-04-30")
      .agg(sum("Deaths").as("Total_April"))

    val dfMay2020 = df.filter(df("ObservationDate") === "2020-05-31")
      .agg(sum("Deaths").as("Total_May"))

    val dfJune2020 = df.filter(df("ObservationDate") === "2020-06-30")
      .agg(sum("Deaths").as("Total_June"))

    val dfJuly2020 = df.filter(df("ObservationDate") === "2020-07-31")
      .agg(sum("Deaths").as("Total_July"))

    val dfAugust2020 = df.filter(df("ObservationDate") === "2020-08-31")
      .agg(sum("Deaths").as("Total_August"))

    val dfSeptember2020 = df.filter(df("ObservationDate") === "2020-09-30")
      .agg(sum("Deaths").as("Total_September"))

    val dfOctober2020 = df.filter(df("ObservationDate") === "2020-10-31")
      .agg(sum("Deaths").as("Total_October"))

    val dfNovember2020 = df.filter(df("ObservationDate") === "2020-11-30")
      .agg(sum("Deaths").as("Total_November"))

    val dfDecember2020 = df.filter(df("ObservationDate") === "2020-12-31")
      .agg(sum("Deaths").as("Total_December"))

    val dfJanuary2021 = df.filter(df("ObservationDate") === "2021-01-31")
      .agg(sum("Deaths").as("Total_January"))

    val dfFebruary2021 = df.filter(df("ObservationDate") === "2021-02-28")
      .agg(sum("Deaths").as("Total_February"))

    val dfMarch2021 = df.filter(df("ObservationDate") === "2021-03-31")
      .agg(sum("Deaths").as("Total_March"))

    val dfApril2021 = df.filter(df("ObservationDate") === "2021-04-30")
      .agg(sum("Deaths").as("Total_April"))

    val february2020Deaths = dfFebruary2020.join(dfJanuary2020).withColumn("February_2020", expr("Total_February-Total_January"))
      .drop("Total_February")
      .drop("Total_January")

    val march2020Deaths = dfMarch2020.join(february2020Deaths).withColumn("March_2020", expr("Total_March-February_2020"))
      .drop("Total_March")
      .drop("February_2020")

    val april2020Deaths = dfApril2020.join(march2020Deaths).withColumn("April_2020", expr("Total_April-March_2020"))
      .drop("Total_April")
      .drop("March_2020")

    val may2020Deaths = dfMay2020.join(april2020Deaths).withColumn("May_2020", expr("Total_May-April_2020"))
      .drop("Total_May")
      .drop("April_2020")

    val june2020Deaths = dfJune2020.join(may2020Deaths).withColumn("June_2020", expr("Total_June-May_2020"))
      .drop("Total_June")
      .drop("May_2020")

    val july2020Deaths = dfJuly2020.join(june2020Deaths).withColumn("July_2020", expr("Total_July-June_2020"))
      .drop("Total_July")
      .drop("June_2020")

    val august2020Deaths = dfAugust2020.join(july2020Deaths).withColumn("August_2020", expr("Total_August-July_2020"))
      .drop("Total_August")
      .drop("July_2020")

    val september2020Deaths = dfSeptember2020.join(august2020Deaths).withColumn("September_2020", expr("Total_September-August_2020"))
      .drop("Total_September")
      .drop("August_2020")

    val october2020Deaths = dfOctober2020.join(september2020Deaths).withColumn("October_2020", expr("Total_October-September_2020"))
      .drop("Total_October")
      .drop("September_2020")

    val november2020Deaths = dfNovember2020.join(october2020Deaths).withColumn("November_2020", expr("Total_November-October_2020"))
      .drop("Total_November")
      .drop("October_2020")

    val december2020Deaths = dfDecember2020.join(november2020Deaths).withColumn("December_2020", expr("Total_December-November_2020"))
      .drop("Total_December")
      .drop("November_2020")

    val january2021Deaths = dfJanuary2021.join(december2020Deaths).withColumn("January_2021", expr("Total_January-December_2020"))
      .drop("Total_January")
      .drop("December_2020")

    val february2021Deaths = dfFebruary2021.join(january2021Deaths).withColumn("February_2021", expr("Total_February-January_2021"))
      .drop("Total_February")
      .drop("January_2021")

    val march2021Deaths = dfMarch2021.join(february2021Deaths).withColumn("March_2021", expr("Total_March-February_2021"))
      .drop("Total_March")
      .drop("February_2021")

    val april2021Deaths = dfApril2021.join(march2021Deaths).withColumn("April_2021", expr("Total_April-March_2021"))
      .drop("Total_April")
      .drop("March_2021")

    val months = february2020Deaths.join(march2020Deaths)
      .join(april2020Deaths)
      .join(may2020Deaths)
      .join(june2020Deaths).join(july2020Deaths).join(august2020Deaths).join(september2020Deaths)
      .join(october2020Deaths).join(november2020Deaths).join(december2020Deaths).join(january2021Deaths)
      .join(february2021Deaths).join(march2021Deaths).join(april2021Deaths)

    val fixedMonthsTable = months.selectExpr("stack(15,'February_2020',February_2020,'March_2020',March_2020,'April_2020',April_2020,'May_2020',May_2020,'June_2020',June_2020," +
      "'July_2020',July_2020,'August_2020',August_2020,'September_2020',September_2020,'October_2020',October_2020,'November_2020',November_2020,'December_2020'," +
      "December_2020,'January_2021',January_2021,'February_2021',February_2021,'March_2021',March_2021,'April_2021',April_2021)")
      .withColumnRenamed("col0", "Months")
      .withColumnRenamed("col1", "Deaths")
      .where("Deaths is not null")

    fixedMonthsTable.show()

    fixedMonthsTable.coalesce(1).write
        .mode(SaveMode.Overwrite)
        .option("header", true)
        .csv(workingPath+"results/q3_PeakOfDeaths")
  }
}