import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IPLDataAnalyzer {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("IPLDataAnalyzer")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")  // Infers the schema of the DataFrame
      .csv("/Users/mukeshbehera/Documents/IPL_2023_DATASET.csv")
    // Show the DataFrame
    val renamedDF = df.withColumnRenamed("_c0", "SL No")
      .withColumnRenamed("COST IN ₹ (CR.)","COST IN ₹")
      .withColumnRenamed("Cost IN $ (000)","Cost IN $")
    val renameDF1 = renamedDF .drop("Base Price IN ₹", "Base Price IN $")
    val cleanedDF = renameDF1.filter(col("2022 Squad").isNotNull)

    val updatedDF = cleanedDF.withColumn("Type", initcap(col("Type")))




    val updatedDF1 = updatedDF
      .withColumn("BasePriceInCrores", when(col("Base Price") === "Retained", lit(0.0))
        .otherwise(col("Base Price") / 100.0))
      .withColumn("AuctionStatus",
        when((col("Base Price") === "Retained") || (col("BasePriceInCrores") =!= col("COST IN ₹")), lit("Y"))
          .otherwise(lit("N")))
      .drop("BasePriceInCrores")

    updatedDF1.show()


    updatedDF1.show(10)
    //df.printSchema()
    //val count =updatedDF.count()
    //println(s"Number of records in the DataFrame: $count")
    //df.show(10)
    spark.stop()

  }
}