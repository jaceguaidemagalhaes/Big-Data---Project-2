package ui

import database.SparkConnection
import database.CRUD.{createAccount, createAccountHidden}
import org.apache.spark.sql.SparkSession
import ui.UI.{adminMenu, basicMenu}

import scala.Console.{BOLD, RESET, println}
import scala.io.StdIn
import scala.util.control.Breaks.breakable
import com.github.t3hnar.bcrypt._
import system.Logging

object main extends App {

  //<editor-fold desc="Spark Session = spark">



  val spark = SparkConnection.sparkConnect()
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("Set hive.exec.dynamic.partition.mode=nonstrict")
  spark.sql("set hive.enforce.bucketing = true")

  //</editor-fold>

  //<editor-fold desc="userAccounts table creation, salt and default admin check">

  //spark.sql("DROP TABLE IF EXISTS userAccounts")
  spark.sql("DROP TABLE IF EXISTS usersTemp")
  val dfAccounts = spark.sql("create table IF NOT EXISTS userAccounts(username STRING, password STRING, permissionType STRING) " +
    "stored as orc")
  val checkForDefaultAdmin = spark.sql("select username from userAccounts where lower(username) = 'admin'")
  val salt = "$2a$10$zLDctXGMbL/R6YkgRA7Nq."
  if (checkForDefaultAdmin.isEmpty) {
    createAccountHidden("admin", "test" ,"admin", spark, salt)
    println("Default admin account created.")
  }

  //</editor-fold>

  //create object for manage logging
  // jaceguai 4/05/2022 4:51 Est
  val logging = new Logging()
  logging.createSysLog()
  //use the following line where you want to log activities
  //just replace Message to log with your message
  //logging.insertLog("Message to log", this.getClass.getSimpleName.toLowerCase())

  var UN = ""
  var input1 = 0
  val menu1 =
    s"""
       |Please choose an option:
       |1. Login
       |2. Create New Account
       |3. Quit
       |""".stripMargin //main menu for user
  lazy val aMenu =
    s"""
       |Please choose an option:
       |1. Use Queries
       |2. Manage Users
       |3. Change Username
       |4. Change Password
       |5. Log Out
       |6. List Log
       |""".stripMargin //admin menu options, change this and admin menu function in UI.scala if needed.
  lazy val bMenu =
    s"""
       |Please choose an option:
       |1. Use Queries
       |2. Change Username
       |3. Change Password
       |4. Log Out
       |""".stripMargin //basic menu options, change this and basic menu function in UI.scala if needed.
  lazy val qMenu =
    s"""
       |Please choose an option:
       |1. What are the top 10 countries with greatest amount of deaths?
       |2. Select percentage of population confirmed, dead, and recovered
       |3. Cumulative average confirmed, deaths, and recovers
       |4. When was the peak of mortality rate of the pandemic?
       |5. What are the correlation between deaths and population?
       |6. What is the average recovered rate by countries?
       |7. Confirmed by day, country, and states (when applicable)
       |8. Total confirmed, death, and recover
       |9. Confirmed spread speed for determined time and country
       |10. Death spread speed for determined time and country
       |11. Exit
       |""".stripMargin //query menu options, change this and query menu function in UI.scala when actual queries are made
  lazy val mMenu =
    s"""
       |Please choose an option:
       |1. Delete User
       |2. Show Users
       |3. Update Privilege of User
       |4. Exit
       |""".stripMargin //manage menu options, change this and manage menu function in UI.scala if needed.

  println(s"${BOLD}Welcome to the COVID data analyzer!$RESET\n") //only prints when program is started
  breakable {
    while (input1 != 3) {
      println(menu1)
      input1 = StdIn.readInt()
      input1 match {
        case 1 => login()
        case 2 => createAccount("basic", spark, salt)
        case 3 => println("Exiting app...")
        case _ => println("Invalid input!")
      }
    }
  }
  println("Thank you for using the COVID data analyzer! Have a great day!")

  def login(spark: SparkSession = spark): Unit = {
    UN = StdIn.readLine("Please enter your username:\n")
    var checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    while (checkIfExist.isEmpty) {
      UN = StdIn.readLine(s"$UN does not exist! Please enter another username:\n")
      checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    }
    var PW = StdIn.readLine("Please enter your password (case-sensitive):\n")
    var checkPW = spark.sql(s"select password from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '${PW.bcryptBounded(salt)}'")
    while (checkPW.isEmpty) {
      PW = StdIn.readLine(s"Password is incorrect! Please try again:\n")
      checkPW = spark.sql(s"select password from userAccounts " +
        s"where lower(username) = '${UN.toLowerCase}' and password = '${PW.bcryptBounded(salt)}'")
    }
    //log user login
    //jaceguai 4/05/2022
    val logging = new Logging()
    logging.insertLog("User logged",this.getClass.getSimpleName.toLowerCase())

    val permission = spark.sql(s"select permissionType from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '${PW.bcryptBounded(salt)}'")
    val checkPermission = permission.head().getString(0)
    checkPermission match {
      case "admin" => println(s"Welcome $UN!"); adminMenu(UN, checkPermission, aMenu, spark, salt)
      case "basic" => println(s"Welcome $UN!"); basicMenu(UN, checkPermission, bMenu, spark, salt)
      case _ => println("Account error! Please contact Customer Support!")
    }
  }

}
