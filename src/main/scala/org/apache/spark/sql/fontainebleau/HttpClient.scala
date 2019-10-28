package org.apache.spark.sql.fontainebleau

import java.io.IOException

import org.apache.http.HttpResponse
import org.apache.http.client.ResponseHandler
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.internal.Logging

/**
 * @time 2019-10-15 10:50
 * @author fchen <cloud.chenfu@gmail.com>
 */
object HttpClient {

  val timeout = 3000

  private lazy val httpClient = {
    val cm = new PoolingHttpClientConnectionManager()
    cm.setDefaultMaxPerRoute(70)
    cm.setMaxTotal(140)
    val conf = RequestConfig.custom()
      .setSocketTimeout(timeout)
      .setConnectionRequestTimeout(timeout)
      .setConnectTimeout(timeout)
      .build()
    HttpClients.custom()
      .setConnectionManager(cm)
      .setDefaultRequestConfig(conf)
      .build()
  }

  @throws[IOException]
  def post(url: String, content: String): String = {
    val method = new HttpPost(url)
    method.setEntity(new StringEntity(content, "UTF-8"))
    httpClient.execute(method, new EventResponseHandler()).getOrElse("")
  }

  @throws[IOException]
  def get(url: String): String = {
    val method = new HttpGet(url)
    httpClient.execute(method, new EventResponseHandler()).getOrElse("")
  }

  class EventResponseHandler extends ResponseHandler[Option[String]] with Logging {
    override def handleResponse(response: HttpResponse): Option[String] = {
      if (response.getStatusLine.getStatusCode == 200) {
        Some(response.getEntity).map(EntityUtils.toString)
      } else {
        logError("Unexpected response status: " + response.getStatusLine.getStatusCode)
        None
      }
    }
  }
}
