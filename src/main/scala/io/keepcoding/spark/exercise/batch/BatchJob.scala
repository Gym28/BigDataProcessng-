package io.keepcoding.spark.exercise.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDeviceWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def totalBytesAntena(dataFrame: DataFrame): DataFrame

  def totalBytestransByUsuario(dataFrame: DataFrame): DataFrame

  def totalBytestransByApp(dataFrame: DataFrame): DataFrame

  def quotapass(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichDeviceWithMetadata(antennaDF, metadataDF).cache()
    val aggByCoordinatesDF = totalBytesAntena(antennaMetadataDF)
    val aggPercentStatusDF = totalBytestransByUsuario(antennaMetadataDF)
    val aggErroAntennaDF = totalBytestransByApp(antennaMetadataDF)

    writeToJdbc(aggByCoordinatesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggPercentStatusDF, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggErroAntennaDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)

    writeToStorage(antennaDF, storagePath)

    spark.close()
  }

}
