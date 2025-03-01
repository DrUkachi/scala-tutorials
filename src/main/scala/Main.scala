import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object Main { 
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Example")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/teamspace/studios/this_studio/scala-work/data/AAPL.csv")
      
    df.printSchema()
    df.show(10)

    import spark.implicits._
    println("Selecting columns using select method")
    df.select("Date", "Open", "Close").show(10)
    println("Selecting columns using col method")
    df.select(col("Date"), col("Open"), col("Close")).show(10)
    println("Selecting columns using $ method")
    df.select($"Date", $"Open", $"Close").show(10)
    println("Selecting columns using $ method with select method")
    df.select(col("Date"), $"Open", df("Close")).show(10)
  }
}