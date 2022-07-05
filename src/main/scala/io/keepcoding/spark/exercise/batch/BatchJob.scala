package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.batch.BatchJobImpl.writeToJdbc
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

case class MobileDevicesMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, antenna_id: String, bytes: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDeviceWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def totalBytesAntena(dataFrame: DataFrame): DataFrame

  def totalBytestransByUsuario(dataFrame: DataFrame): DataFrame

  def totalBytestransByApp(dataFrame: DataFrame): DataFrame

  def quotapass(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcbytesbyHour, aggJdbcquotalimit, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val deviceDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val deviceMetadataDF = enrichDeviceWithMetadata(deviceDF, metadataDF).cache()

    val aggtotalBytesAntenaDF = totalBytesAntena(deviceMetadataDF)
    val aggtotalBytestransByUsuarioDF = totalBytestransByUsuario(deviceMetadataDF)
    val aggtotalBytestransByApDF = totalBytestransByApp(deviceMetadataDF)
    val aggquotapassDF= quotapass(deviceMetadataDF)


        writeToJdbc(aggtotalBytesAntenaDF , jdbcUri, aggJdbcbytesbyHour,jdbcUser, jdbcPassword)
        writeToJdbc(aggtotalBytestransByUsuarioDF , jdbcUri, aggJdbcbytesbyHour, jdbcUser, jdbcPassword)
        writeToJdbc(aggtotalBytestransByApDF, jdbcUri, aggJdbcbytesbyHour, jdbcUser, jdbcPassword)
        writeToJdbc(aggquotapassDF, jdbcUri, aggJdbcquotalimit, jdbcUser, jdbcPassword)
    spark.close()
  }

}
