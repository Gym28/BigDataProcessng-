package io.keepcoding.spark.exercise.batch

import org.apache.spark.sql.functions.{lit, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object BatchJobImpl extends BatchJob {
  override val spark: SparkSession =
    SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Structured Streaming KeepCoding Base")
    .getOrCreate()
  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    //hacemos un job
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where($"year"===lit(filterDate.getYear)&&
        $"month"=== lit(filterDate.getMonthValue) &&
        $"day" === lit(filterDate.getDayOfMonth)&&
        $"hour" === lit(filterDate.getHour))
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option ("url",jdbcURI)
      .option("dbtable",jdbcTable)
      .option("user", user)
      .option("password",password)
      .load()
  }

  override def enrichDeviceWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame ={
    antennaDF.as("a")
      .join(metadataDF.as("b"), $"a.id"===$"b.id" )
      //para no tener dos columnas de id, borramos uno
      .drop($"b.id")


  }

  override def totalBytesAntena(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"id", $"value", $"type")
      .where($"type" === lit("antena_by_total"))
      .groupBy($"id", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as ("value")
      ).withColumn("type", lit("Antena_bytes_total"))
      .select($"id", $"window.start".as("date") , $"value", $"type")


  }

  override def totalBytestransByUsuario(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"id", $"bytes", $"email")
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as ("value")

      ).withColumn("type", lit("user_bytes_total"))
      .select($"email".as("id"), $"window.start".as("date") , $"value", $"type")

  }

  override def totalBytestransByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"app", $"bytes")
      .groupBy($"app", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as ("value")
      ).withColumn("type", lit("app_bytes_total"))
      .select($"app".as("id"), $"window.start".as("date") , $"value", $"type")
  }

  override def quotapass(dataFrame: DataFrame): DataFrame = ???

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = ???

  def main (args: Array[String]): Unit ={
    var rawDF= readFromStorage("/tmp/spark-data", OffsetDateTime.parse("2022-02-27T22:00:00Z"))
    val metadataDF = readAntennaMetadata( s"jdbc:postgresql://localhost:5432/postgres",
      "user_metadata",
      "postgres",
      "mysecretpassword" )
    val enrichDF = enrichDeviceWithMetadata(rawDF, metadataDF)

    writeToJdbc(totalBytestransByUsuario(enrichDF),
      s"jdbc:postgresql://localhost:5432/postgres",
      "bytesByHour",
      "postgres",
      "mysecretpassword"

    )
    writeToJdbc(totalBytesAntena(enrichDF),
      s"jdbc:postgresql://localhost:5432/postgres",
      "bytesByHour",
      "postgres",
      "mysecretpassword")

    writeToJdbc(totalBytestransByApp(enrichDF),
      s"jdbc:postgresql://localhost:5432/postgres",
      "bytesByHour",
      "postgres",
      "mysecretpassword")

  }





}
