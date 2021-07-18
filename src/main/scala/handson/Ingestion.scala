package handson

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{first, input_file_name, regexp_replace, split, substring, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import handson.conf.Configuration.config

import scala.annotation.tailrec

object Ingestion {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark ingestion app")
      .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val inputDF = spark
      .read
      .option("delimiter", "\t")
      .csv(config.inputPath)

    val outputDF = getOutputDF(inputDF)

    outputDF
      .write
      .mode(SaveMode.Overwrite)
      .parquet(config.outputPath)
  }

  def getOutputDF(inputDF: DataFrame): DataFrame = {
    val tempDF = getTempDF(inputDF)
    getColumns(tempDF, 1)
      .drop("_c0", "data")
  }

  def getTempDF(inputDF: DataFrame): DataFrame = inputDF
    .withColumn("year", when($"_c0".startsWith("@relation"), substring($"_c0", 12, 1)).cast(IntegerType))
    .withColumn("year", first("year") over Window.partitionBy(input_file_name()))
    .filter(!$"_c0".startsWith("@"))
    .withColumn("data", regexp_replace($"_c0", "\\?", ""))
    .withColumn("data", split($"data", ","))
    .withColumn("class", $"data"(64).cast(IntegerType))

  @tailrec
  def getColumns(df: DataFrame, attrNumber: Int): DataFrame =
    if (attrNumber > 64) df
    else getColumns(getColumn(df, attrNumber), attrNumber + 1)

  def getColumn(df: DataFrame, attrNumber: Int): DataFrame =
    df
      .withColumn(s"Attr${attrNumber}", $"data"(attrNumber - 1).cast(DoubleType))
}
