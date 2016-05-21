name := "Spark-SPARQLConnector"

version := "1.0"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.10.5", "2.11.7")



//val testSparkVersion = settingKey[String]("The version of Spark to test against.")
//
//testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)
//
//sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.apache.jena" % "jena-jdbc-driver-remote" % "3.0.1",
  "org.apache.jena" % "jena-jdbc-driver-mem"    % "3.0.1",
  "org.slf4j"       % "slf4j-api"               % "1.7.5" % Provided,
  "org.scalatest"  %% "scalatest"               % "2.2.1" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" ,
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.scala-lang" % "scala-library" % "2.11.7"% Provided
)