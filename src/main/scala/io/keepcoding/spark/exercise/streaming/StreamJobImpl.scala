package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object StreamJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Structured Streaming KeepCoding Base")
    .getOrCreate()
  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", " localhost:9092")
      .option("subscribe", topic)
      .load()


  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    // estructura que tendrá nuestra tabla usando los datos y la memoria, según
    // nuestra memoria serán los siguientes, según su tipo el structType requiere el dato, el tipo de dato y que no será nulo
    // es importante usar el timestamp casteada o modificando su type a Timestamp para poder usarlo para las agregaciones de
    // windows
    val struct = StructType(Seq(
      StructField ("timestamp", TimestampType,   nullable = false),
      StructField ("id",StringType, nullable=false),
      StructField ("antenna_id", StringType, nullable = false),
      StructField ("bytes",LongType, nullable=false),
      StructField ("app", StringType, nullable=false),
      //StructField ("metric", StringType, nullable = false),
     // StructField("value", IntegerType, nullable = false)

    ))
// parseamos a string la columna value para que los datos aparezcan en string y
    //tome la estructura de nuestra tabla definida en Struct
    dataFrame
      .select(from_json($"value".cast(StringType), struct).as("value"))
      //tomamos la información que tiene value para quitar este nivel y aparezcan todos los datos en columnas
      .select($"value.*")

  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option ("url",jdbcURI)
      .option("dbtable",jdbcTable)
      .option("user", user)
      .option("password",password)
      .load()


  }

  override def enrichDevicesWithUserMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("a")
      .join(metadataDF.as("b"), $"a.id"===$"b.id" )
     //para no tener dos columnas de id, borramos uno
      .drop($"b.id")


  }



  override def totalbytesAntena(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp", "5 seconds")
      .groupBy($"antenna_id", window($"timestamp", "90 seconds"))
      .agg(
          sum($"bytes").as ("value")
      ).withColumn("type", lit("Antena_bytes_total"))
      .select($"antenna_id".as("id"), $"window.start".as("timestamp") , $"value", $"type")


  }

  override def totalbytesporUser(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"id", $"bytes", $"name")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"name", window($"timestamp", "90 seconds"))
      .agg(
        sum($"bytes").as ("value")

      ).withColumn("type", lit("user_bytes_total"))
      .select($"name".as("id"), $"window.start".as("timestamp") , $"value", $"type")


  }

  override def totalbytesAplicacion(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"app", window($"timestamp", "90 seconds"))
      .agg(
        sum($"bytes").as ("value")
      ).withColumn("type", lit("app_bytes_total"))
      .select($"app".as("id"), $"window.start".as("timestamp") , $"value", $"type")


  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future{
    dataFrame
      .writeStream
      .foreachBatch{(data:DataFrame, batchId:Long )=>
        data
          .write
          .format("jdbc")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .mode(SaveMode.Append)
          .save()
      } .start()
      .awaitTermination()

  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future{
    dataFrame
      //añadimos las columnas para el particionado
      //timestamp es para obtener el año, mes y dia
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      //donde voy a escribir
      .option("path", storageRootPath)
      .option("checkpointLocation", "/tmp/spark-checkPoint5")
      //hacemos un start
      .start()
      .awaitTermination()
  }

  def main (args: Array[String]): Unit ={
  val metadataDF = readUserMetadata(
    s"jdbc:postgresql://localhost:5432/postgres",
    "user_metadata",
    "postgres","mysecretpassword")

    val future1= writeToJdbc(totalbytesAntena(
   enrichDevicesWithUserMetadata(
     parserJsonData(
       readFromKafka("192.168.18.123:9092", "devices")),

     metadataDF
   )
    ),s"jdbc:postgresql://localhost:5432/postgres","bytes", "postgres","mysecretpassword")

    val future2= writeToJdbc(totalbytesporUser(
      enrichDevicesWithUserMetadata(
        parserJsonData(
          readFromKafka("192.168.18.123:9092", "devices")),

        metadataDF
      )
    ),s"jdbc:postgresql://localhost:5432/postgres","bytes", "postgres","mysecretpassword")
    val future3= writeToJdbc(totalbytesAplicacion(
      enrichDevicesWithUserMetadata(
        parserJsonData(
          readFromKafka("192.168.18.123:9092", "devices")),

        metadataDF
      )
    ),s"jdbc:postgresql://localhost:5432/postgres","bytes", "postgres","mysecretpassword")

    val future4 =writeToStorage(parserJsonData(readFromKafka("192.168.18.123:9092", "devices")), "/tmp/spark-data")
    //debemos hacer el Await para lanzar los futuros,
    // hacemos una secuencia de futuros
    Await.result(Future.sequence(Seq(future1, future2, future3, future4)), Duration.Inf)



  }



}
