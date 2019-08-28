package org.apache.spark.sql.fontainebleau

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * @time 2019-08-28 22:44
 * @author fchen <cloud.chenfu@gmail.com>
 */
object RestApiSourceExample {
  val URL = "http://httpbin.org/get"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .getOrCreate()

    spark.readStream
      .format("rest")
      .option("url", URL)
      .load
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
  }
}
