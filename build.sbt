name := "keon-spark-command"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"