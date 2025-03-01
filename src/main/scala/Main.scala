import org.apache.spark.sql.SparkSession

object Main { 
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Example")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .csv("/teamspace/studios/this_studio/scala-work/data/AAPL.csv")

    df.printSchema()
    df.show()
    spark.stop()
  }
}