package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class DeviceMessage(timestamp: Timestamp, id: String, antenna_id: String,bytes: Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDevicesWithUserMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def totalbytesAntena(dataFrame: DataFrame): DataFrame

  def totalbytesporUser(dataFrame: DataFrame): DataFrame

  def totalbytesAplicacion(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichDevicesWithUserMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)
    val aggByCoordinatesDF = totalbytesAntena(antennaMetadataDF)
    val aggFuture = writeToJdbc(aggByCoordinatesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(aggFuture, storageFuture)), Duration.Inf)

    spark.close()
  }

}
