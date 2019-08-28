package org.apache.spark.sql.fontainebleau

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

/**
 * @time 2019-08-28 22:52
 * @author fchen <cloud.chenfu@gmail.com>
 */
class RestApiDataSource extends DataSourceRegister with StreamSourceProvider with Logging {
  override def shortName(): String = "rest"

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    val _schema = new RestApiSource(url(parameters)).schema
    (shortName(), _schema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    new RestApiSource(url(parameters))
  }

  def url(parameters: Map[String, String]): String = {
    parameters.getOrElse(
      "url",
      throw new IllegalArgumentException("set url configuration for RestApiDataSource")
    )

  }
}
