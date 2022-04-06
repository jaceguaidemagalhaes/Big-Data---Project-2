ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"


lazy val root = (project in file("."))
  .settings(
    name := "Project2"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.2" % "provided"
libraryDependencies += "com.github.t3hnar" %% "scala-bcrypt" % "4.3.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}




