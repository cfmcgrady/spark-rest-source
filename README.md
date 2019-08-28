[![License](http://img.shields.io/:license-Apache_v2-blue.svg)](https://github.com/cfmcgrady/spark-rest-source/blob/master/LICENSE)

# Spark Rest Source

A Rest Api Structured Streaming DataSource 

# Example

```scala
spark.readStream
  .format("rest")
  .option("url", URL)
  .load
  .writeStream
  .format("console")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()
  .awaitTermination()
```
Here is a [full example](src/main/scala/org/apache/spark/sql/fontainebleau/RestApiSourceExample.scala).
