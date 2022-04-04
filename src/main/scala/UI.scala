import org.apache.spark.sql.SparkSession
import scala.Console._
import queries._
import CRUD._
import scala.io.StdIn
import scala.util.control.Breaks._
import main.{aMenu,bMenu,qMenu,mMenu}


object UI extends App {

  def adminMenu (UN: String, permission: String, menu: String = aMenu, spark: SparkSession, salt: String) : Unit = {
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
          case _ => println("Invalid input!")
        }
      }
    }
  }

  def basicMenu (UN:String, permission: String, menu: String = bMenu, spark: SparkSession, salt: String) : Unit = {
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

  def queryMenu (menu: String, spark: SparkSession) : Unit = {
    var input = 0
    while (input != 7) {
      println(menu)
      println("Query options are currently disabled until COVID queries are made.")
      input = StdIn.readInt()
      input match {
        /*case 1 => queryHighestPeak(spark)
        case 2 => queryLowestPeak(spark)
        case 3 => queryCurrentHighest(spark)
        case 4 => queryHighestInX(spark)
        case 5 => queryHighestPlayerMonth(spark)
        case 6 => queryTopAverageInGame(spark)*/
        case 7 => println("Exiting...")
        case _ => println("Invalid input!")
      }
    }
  }

  def manageUserMenu (menu: String, spark: SparkSession, salt: String) : Unit = {
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
