import system.Logging

object main extends App{
  var UN = "Test main 1"

  val logging = new Logging()
  logging.InsertLog("log from main 2",UN, this.getClass.getSimpleName.toLowerCase())
  logging.ListLog()
}
