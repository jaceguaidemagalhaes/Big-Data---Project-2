package ui

import org.apache.spark.sql.SparkSession
import ui.main.{aMenu, bMenu, mMenu, qMenu}
import database.CRUD._
import query._
import system.Logging

import scala.Console.println
import scala.io.StdIn
import scala.util.control.Breaks.{break, breakable}

object UI extends App {
  //create object for manage logging
  // jaceguai 4/05/2022 4:51 Est
  //use the following line where you want to log activities
  //just replace Message to log with your message
  //val logging = new Logging()
  //logging.insertLog("Message to log", this.getClass.getSimpleName.toLowerCase())
  def adminMenu(UN: String, permission: String, menu: String = aMenu, spark: SparkSession, salt: String): Unit = {
    var input = 0
    println("Welcome to the Admin Menu. Please enter a valid number to select an option:")
    breakable {
      while (input != 5) {
        println(menu)
        input = StdIn.readInt()
        input match {
          case 1 => queryMenu(qMenu, spark)
          case 2 => manageUserMenu(mMenu, spark, salt)
          case 3 => updateUserName(UN, permission, spark, salt); println("Logging out to update username in session..."); break()
          case 4 => updatePassword(UN, permission, spark, salt)
          case 5 => println("Logging out...")
          case 6 => {val logging = new Logging()
                      logging.listLog()}
          case _ => println("Invalid input!")
        }
      }
    }
  }

  def basicMenu(UN: String, permission: String, menu: String = bMenu, spark: SparkSession, salt: String): Unit = {
    println("Welcome to the User Menu. Please enter a valid number to select an option:")
    var input = 0
    breakable {
      while (input != 4) {
        println(menu)
        input = StdIn.readInt()
        input match {
          case 1 => queryMenu(qMenu, spark)
          case 2 => updateUserName(UN, permission, spark, salt); println("Logging out to update username in session..."); break()
          case 3 => updatePassword(UN, permission, spark, salt)
          case 4 => println("Logging out...")
          case _ => println("Invalid input!")
        }
      }
    }
  }

  def queryMenu(menu: String, spark: SparkSession): Unit = {
    var input = 0
    while (input != 11) {
      println(menu)
      println("Query options are currently disabled until COVID queries are made.")
      input = StdIn.readInt()
      input match {
        //case 1 => queryHighestPeak(spark)
        //case 2 => {val q1_PercentageOfPopConfirmed = new Q1_PercentageOfPopConfirmed()
          //          q1_PercentageOfPopConfirmed.executeQuery(spark)}
        /*case 3 => queryAvg(spark)
        case 4 => queryHighestDeath(spark)
        case 5 =>
        case 6 => queryAverageRecoveredRate(spark)
        case 7 =>
        case 8 =>
        case 9 => queryConSpreadSpeed(spark)
        case 10 => queryDeathSpreadSpeed(spark)*/
        case 11 => println("Exiting...")
        case _ => println("Invalid input!")
      }
    }
  }

  def manageUserMenu(menu: String, spark: SparkSession, salt: String): Unit = {
    var input = 0
    while (input != 4) {
      println(menu)
      input = StdIn.readInt()
      input match {
        case 1 => deleteUser(spark)
        case 2 => readAccounts(spark)
        case 3 => updatePrivilege("admin", spark, salt)
        case 4 => println("Exiting...")
        case _ => println("Invalid input!")
      }
    }
  }
}
