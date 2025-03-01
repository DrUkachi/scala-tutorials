import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._


object Main { 
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Example")
      .master("local[*]")
      .getOrCreate()

    val products: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv("/teamspace/studios/this_studio/scala-work/data/products.tsv")
      
    val reviews: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv("/teamspace/studios/this_studio/scala-work/data/reviews.tsv")
    
    val products_with_reviews = reviews.select("id").distinct().collect().map(n => n.getString(0)).toSeq // to get a sequence of product id strings
    
    val prodcucts_without_reviews = products.filter(!col("id").isin(products_with_reviews: _*))

    val kitchen_products = prodcucts_without_reviews.filter(col("category") === "Kitchen") // To get kitchen products without reviews

    val mean_word_count = kitchen_products.withColumn("word_count", size(split(col("description"), " ")))
                                                .agg(mean("word_count").as("mean_word_count"))
                                                .collect()(0).getDouble(0)
                                                // To get mean word count of kitchen products without reviews

    println("Mean word count of kitchen products without reviews:")
    println(mean_word_count)
                                





  }
}