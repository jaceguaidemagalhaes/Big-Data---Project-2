import org.apache.spark.sql.SparkSession
import scala.Console._
import scala.io.StdIn
import com.github.t3hnar.bcrypt._

object CRUD extends App{
  def createAccountHidden(UN: String, PW: String, permission: String, spark: SparkSession, salt: String) : Unit = {
    val checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    if (checkIfUnique.isEmpty) {
      spark.sql(s"insert into userAccounts VALUES ('$UN','${PW.bcryptBounded(salt)}','$permission')")
    }
    else {
      val retryUN = StdIn.readLine(s"$UN has already been taken! Please enter another username:\n")
      createAccountHidden(retryUN, PW, permission, spark, salt)
    }
  }

  def createAccount(permission: String, spark: SparkSession, salt: String) : Unit = {
    val UN = StdIn.readLine("Please enter a unique username:\n")
    val checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    if (checkIfUnique.isEmpty) {
      val PW = StdIn.readLine("Please enter a strong password:\n")
      spark.sql(s"insert into userAccounts VALUES ('$UN','${PW.bcryptBounded(salt)}','$permission')")
      if (permission == "basic")
        println(s"Account $UN created!")
    }
    else {
      println(s"$UN has already been taken!\n")
      createAccount(permission, spark, salt)
    }
  }

  def readAccounts(spark : SparkSession) : Unit = {
    spark.sql("select username as Username, password as Password, permissionType as Privileges from userAccounts" +
      " order by privileges, Username").show(false)
  }

  def updatePassword(UN: String, permission: String, spark: SparkSession, salt: String) : Unit = {
    val oldPW = StdIn.readLine("Please enter your current password:\n")
    val checkPW = spark.sql(s"select password from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '${oldPW.bcryptBounded(salt)}'")
    if (!checkPW.isEmpty) {
      val newPW = StdIn.readLine("Please enter a new password: \n")
      createUserAccountsCopy(spark)
      spark.sql(s"insert into usersTemp select * from userAccounts where lower(username) != '${UN.toLowerCase}'")
      spark.sql("drop table userAccounts")
      spark.sql("alter table usersTemp rename to userAccounts")
      spark.sql(s"insert into table userAccounts VALUES ('$UN','${newPW.bcryptBounded(salt)}','$permission')") //bug: will change UN to whatever was passed into function, shouldn't be an issue when using current username
      println(s"${BOLD}Password has been updated to $newPW!$RESET")
      spark.sql(s"select * from userAccounts where lower(username) = '${UN.toLowerCase}'").show(false)
    }
    else {
      println(s"$oldPW is incorrect!\n")
      updatePassword(UN, permission, spark, salt)
    }
  }

  def updateUserName(UN: String, permission: String, spark: SparkSession, salt: String) : Unit = {
    val PW = StdIn.readLine("Please enter your current password:\n")
    val checkPW = spark.sql(s"select password from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '${PW.bcryptBounded(salt)}'")
    if (!checkPW.isEmpty) {
      var newUN = StdIn.readLine("Please enter a new username: \n")
      var checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${newUN.toLowerCase}'")
      while (!checkIfUnique.isEmpty) {
        newUN = StdIn.readLine(s"$newUN is taken! Please enter another username: \n")
        checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${newUN.toLowerCase}'")
      }
      createUserAccountsCopy(spark)
      spark.sql(s"insert into usersTemp select * from userAccounts where lower(username) != '${UN.toLowerCase}'")
      spark.sql("drop table userAccounts")
      spark.sql("alter table usersTemp rename to userAccounts")
      spark.sql(s"insert into table userAccounts VALUES ('$newUN','${PW.bcryptBounded(salt)}','$permission')")
      println(s"${BOLD}Username has been updated to $newUN!$RESET")
      spark.sql(s"select * from userAccounts where lower(username) = '${newUN.toLowerCase}'").show(false)
      def setUsername (oldUser: String, newUser: String) : Unit = {
        val oldUser = newUser
      }
      setUsername(UN, newUN)
    }
    else {
      println(s"$PW is incorrect!\n")
      updateUserName(UN, permission, spark, salt)
    }
  }

  def updatePrivilege(permission: String, spark: SparkSession, salt: String) : Unit = {
    readAccounts(spark)
    var UN = StdIn.readLine("Please enter a valid username: \n")
    var checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    var getPW = spark.sql(s"select password from userAccounts where lower(username) = '${UN.toLowerCase}'")
    var PW = getPW.head().getString(0)
    while (checkIfUnique.isEmpty) {
      UN = StdIn.readLine(s"$UN doesn't exist! Please enter a valid username:\n")
      checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
      getPW = spark.sql(s"select password from userAccounts where lower(username) = '${UN.toLowerCase}'")
      PW = getPW.head().getString(0)
    }
    createUserAccountsCopy(spark)
    spark.sql(s"insert into usersTemp select * from userAccounts where lower(username) != '${UN.toLowerCase}'")
    spark.sql("drop table userAccounts")
    spark.sql("alter table usersTemp rename to userAccounts")
    spark.sql(s"insert into table userAccounts VALUES ('$UN','${PW.bcryptBounded(salt)}','$permission')")
    println(s"${BOLD}$UN has been updated to $permission status!$RESET")
    spark.sql(s"select * from userAccounts where lower(username) = '${UN.toLowerCase}'").show(false)
  }

  def deleteUser(spark: SparkSession) : Unit = {
    readAccounts(spark)
    var UN = StdIn.readLine("Please enter the user you wish to delete:\n")
    var checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    while (checkIfExist.isEmpty) {
      UN = StdIn.readLine(s"$UN does not exist! Please enter another username:\n")
      checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    }
    createUserAccountsCopy(spark)
    spark.sql(s"insert into usersTemp select * from userAccounts where lower(username) != '${UN.toLowerCase}'")
    spark.sql("drop table userAccounts")
    spark.sql("alter table usersTemp rename to userAccounts")
    println(s"${BOLD}$UN has been deleted successfully!$RESET")
    readAccounts(spark)
  }

  def createUserAccountsCopy(spark: SparkSession, table_name : String = "usersTemp") : Unit = {
    spark.sql(s"create table if not exists $table_name(username STRING, password STRING, permissionType STRING)" +
      s"stored as orc")
  }
}
