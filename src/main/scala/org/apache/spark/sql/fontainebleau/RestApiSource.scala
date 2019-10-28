package org.apache.spark.sql.fontainebleau

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.StructType

/**
 * @time 2019-08-28 22:24
 * @author fchen <cloud.chenfu@gmail.com>
 */
class RestApiSource(url: String) extends Source {
  private var _schema: Option[StructType] = None
  val sparkSession = SparkSession.active
  import sparkSession.implicits._

  override def schema: StructType = {
    _schema.getOrElse {
      val json = HttpClient.get(url)
      val df = sparkSession.read.json(Seq(json).toDS)
      _schema = Option(df.schema)
      df.schema
    }
  }

  private var currentOffset: Long = 0L
  override def getOffset: Option[Offset] = {
    currentOffset += 1
    Option(LongOffset(currentOffset))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    getRequest
  }

  private def getRequest(): DataFrame = {

    val json = HttpClient.get(url)
    val plan = sparkSession.read
      .json(Seq(json).toDS)
      .logicalPlan
      .asInstanceOf[LogicalRDD]
      .copy(isStreaming = true)(sparkSession)
      .asInstanceOf[LogicalPlan]
    Dataset.ofRows(sparkSession, plan)
  }

  override def stop(): Unit = {
    // do nothing.
  }

}
