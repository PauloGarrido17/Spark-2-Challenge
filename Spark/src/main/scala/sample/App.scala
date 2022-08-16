package sample

import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, concat_ws, date_format, explode, regexp_replace, substring, to_date, translate, udf, upper, when}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType}

/**
 * @author ${user.name}
 */
object App {
  def main(args: Array[String]) {

    // Start Spark
    val master = "local[*]"
    val spark: SparkSession = SparkSession.builder()
      .master(master).appName("app")
      .getOrCreate()

    //    --------   Part 1   --------
    // Get csv info
    var user_reviews = spark.read.csv("./csv/googleplaystore_user_reviews.csv")
    //user_reviews.show()

    // Replace default values for 0
    user_reviews = user_reviews.na.replace("_c3", Map("" -> "0"))
    user_reviews = user_reviews.na.replace("_c3", Map("null" -> "0"))
    user_reviews = user_reviews.na.replace("_c3", Map("nan" -> "0"))

    // Select columns to use
    var df1 = user_reviews.select(col("_c0"), col("_c3").cast("double"))

    // Average of the column Sentiment_Polarity grouped by App name
    df1 = df1.groupBy("_c0").avg("_c3")

    // Change names of the columns
    df1 = df1.toDF("App", "Average_Sentiment_Polarity")

    // Show df
    df1.show(false)


    //    --------   Part 2   --------
    // Get csv info
    var gps = spark.read.csv("./csv/googleplaystore.csv")
    gps.show()
    // Replace default values for 0
    gps = gps.na.replace("_c2", Map("nan" -> "0"))
    gps = gps.na.replace("_c2", Map("NaN" -> "0"))
    gps = gps.na.replace("_c2", Map("" -> "0"))
    gps = gps.na.replace("_c2", Map("null" -> "0"))

    // Change Rating to double
    var df2 = gps.withColumn("_c2", col("_c2").cast(DoubleType)).as("_c2")

    // Get only 4.0 >=
    df2 = gps.filter(gps("_c2") >= 4)

    // Sort in desc order
    df2 = df2.sort(col("_c2").desc)

    df2.show()

    // Save to CSV
    df2.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .save("./csv/best_apps")


    //    --------   Part 3   --------

    var gps2 = spark.read.csv("./csv/googleplaystore.csv")

    gps2 = gps2.na.replace("_c2", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c2", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c3", Map("NaN" -> "0"))
    gps2 = gps2.na.replace("_c4", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c5", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c6", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c7", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c8", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c9", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c10", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c11", Map("NaN" -> "null"))
    gps2 = gps2.na.replace("_c12", Map("NaN" -> "null"))
    gps2 = gps2.withColumn("_c2", col("_c2").cast(DoubleType))
    //gps.show()
    var cat = gps2.groupBy("_c0").agg(concat_ws(",", collect_set(col("_c1"))).as("_c1"))

    cat = cat.withColumn("_c1", functions.split(col("_c1"), ",").cast("array<string>"))

    //gps.show(false)

    var maxReviews = gps2.groupBy("_c0","_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12").max("_c2")

    cat.createOrReplaceTempView("cat")
    maxReviews.createOrReplaceTempView("maxReviews")
    var df3 = spark.sql(
      "SELECT c._c0, c._c1, r._c2, r._c3, r._c4, r._c5, r._c6, r._c7, r._c8, r._c9, r._c10, r._c11, r._c12 " +
        "FROM cat c, maxReviews r WHERE r._c0 == c._c0")

    // Change column types
    // Rating
    df3 = df3.withColumn("_c2", col("_c2").cast(DoubleType))
    // Reviews
    df3 = df3.withColumn("_c3", col("_c3").cast(LongType))

    // Price
    df3 = df3.withColumn("_c7", translate(df3.col("_c7"),"M",""))
    df3 = df3.withColumn("_c7", translate(df3.col("_c7"),"$",""))
    df3 = df3.withColumn("_c7", col("_c7").cast(DoubleType) * 0.9)

    // Size
    df3 = df3.withColumn("_c4", translate(df3.col("_c4"),"M",""))
    df3 = df3.withColumn("_c4", translate(df3.col("_c4"),"K",""))
    df3 = df3.withColumn("_c4", col("_c4").cast(DoubleType))

    // Genres
    df3 = df3.withColumn("_c9", functions.split(col("_c9"), ";").cast("array<string>"))

    // Date
    df3 = df3.withColumn("_c10", to_date(col("_c10"), "MMMM d, yyyy"))

    // Change columns names
    df3 = df3.toDF("App", "Categories", "Rating", "Reviews", "Size", "Installs", "Type", "Price",
      "Content_Rating", "Genres", "Last_Updated", "Current_Version", "Minimum_Android_Version")

    df3.sort("App").show(false)

    //    --------   Part 4   --------

    df3.createOrReplaceTempView("df3")
    df1.createOrReplaceTempView("df1")
    var df4 = spark.sql("SELECT df3.*, df1.Average_Sentiment_Polarity FROM df3, df1 WHERE df1.App == df3.App")

    // Save as parquet
    df4.coalesce(1).write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "gzip")
      .save("googleplaystore_cleaned")

    df4.show()
    //    --------   Part 5   --------

    df3.createOrReplaceTempView("df3")
    df1.createOrReplaceTempView("df1")
    var df5 = spark.sql("SELECT df3.*, df1.Average_Sentiment_Polarity FROM df3 LEFT JOIN df1 on df1.App == df3.App")

    // Change default values to null
    df5 = df5.na.replace("_c2", Map("nan" -> "null"))
    df5 = df5.na.replace("_c2", Map("NaN" -> "null"))
    df5 = df5.na.replace("_c2", Map("" -> "null"))

    df5 = df5.select(col("App"), col("Categories"), col("Rating").cast(LongType), col("Reviews"),
      col("Size"), col("Installs"), col("Type"), col("Price"), col("Content_Rating"),
      explode(col("Genres")).alias("Genres"), col("Last_Updated"),
      col("Current_Version"), col("Minimum_Android_Version"), col("Average_Sentiment_Polarity"))

    df5 = df5.groupBy("Genres").agg(
      "App" -> "count",
      "Rating" -> "avg",
      "Average_Sentiment_Polarity" -> "avg"
    )

    // Change column names
    df5 = df5.toDF("Genre", "Count", "Average_Rating", "Average_Sentiment_Polarity")

    df5.show()

    // Save to Parquet
    df5.coalesce(1).write
      .format("parquet")
      .mode("overwrite")
      .option("compression", "gzip")
      .save("googleplaystore_metrics")
  }

}
