package system

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Calendar


//object system.Logging extends App {
class Logging{

  //creating class to manage logging.
  // System will get the user and save its username, date end time of operation, classname and message to save
  //trello task: 8
  // Jaceguai 04/01/2022 3:29 EST

  //val user = "test log" // user variable to test reasons comment for test.
  //val user = main.UN// uncomment for production
  var sysDate = ""
  var sysTime = ""
  var sysTimeZone = ""


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
  val sc = spark.sparkContext

  //InsertLog("test insert log 2")
  //ListLog

  //insert log data into syslog table
  //trello 8
  //Jaceguai 4/04/2022 12:52 EST
  def insertLog(logmessage: String,user: String, pclassName: String):Unit= {
    getCurrentTime
    //val className = ClassName
    println("Saving log....")
    spark.sql(s"insert into table syslog (username, classname, date, time, timezone, operation) " +
      s" values ('$user', '$pclassName', '$sysDate', '$sysTime', '$sysTimeZone', '$logmessage')")
  }

  //list log data from syslog table
  //Trello 8
  //Jaceguai 4/04/2022
  def listLog():Unit= {
    getCurrentTime
    println("Listing log....")
    spark.sql("select * from syslog order by date, time, operation").show()
  }


  //creates table syslog
  //trello 8
  //jaceguai 4/04/2022 11:33 EST
  def createSysLog():Unit={
    println("Creating table syslog....")
    spark.sql("create table if not exists syslog(username string, classname string, date string," +
      "time string, timezone string, operation string)"+
      " row format delimited fields terminated by ','" +
      " stored as textfile")
    spark.sql("describe syslog").show()
  }

  //method to get the name of the class
  //trello task 8,
  //Jaceguai 04/01/2022 3:35 EST
  def className = this.getClass.getSimpleName.toLowerCase()


  //method to get currenttime. return keeps global strings  sysDate (YYYY/mm/dd), sysTime (hh:mm:ss), and sysTimeZone (name of the time zone)
  //trello task 8
  //jaceguai 04/01/2022 3:35 EST
  def getCurrentTime: Unit = {
    val now = Calendar.getInstance()
    //val monthFormat = new SimpleDateFormat("mm")
    val hour = now.get(Calendar.HOUR_OF_DAY)
    val minute = now.get(Calendar.MINUTE)
    val seconds = now.get(Calendar.SECOND)
    val day = now.get(Calendar.DATE)
    val month = now.get(Calendar.MONTH)
    val year = now.get(Calendar.YEAR)
    val timeZone = now.getTimeZone

    try {
      //return year+"-"+month+"-"+day+" "+hour+":"+minute+":"+seconds+" "+timeZone.getID
      sysDate = f"$year-$month%02d-$day%02d"
      sysTime = f"$hour%02d:$minute%02d:$seconds%02d"
      sysTimeZone = timeZone.getID
      //val currentDateTime = Map("Date" -> f"$year-$month%02d-$day%02d","Time" -> f"$hour%02d:$minute%02d:$seconds%02d","TimeZone" -> timeZone.getID)
      //return currentDateTime
      //return "" + currentHour
    } catch {
      case e: Throwable => e.printStackTrace
        println("Error getting hour")
        val currentDate = Map(("Date","0"),("Time","0"),("TimeZone","Error"))
        //return currentDate
    }
    //return hourFormat.format(now)
  //end GetCurrentTime
  }

  //end logging
}
