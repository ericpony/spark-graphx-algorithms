scalaVersion := "2.11.6"

initialCommands += """
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val sc = new SparkContext("local[*]", "Console")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
  """.stripMargin

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.1"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
