// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter

val event_hub_conn_string = "connection_string_here" // português: substitua com a string de conexão // español: Reemplazar con la cadena de conexión // english: replace with the string connection // deustch: Ersetzen Sie mit der Verbindungszeichenfolge 

val event_hub_conf = s"""{
    'eventhubs.connectionString' : ${event_hub_conn_string}
}"""

val connectionString = ConnectionStringBuilder(event_hub_conn_string).build 
val ehConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

val tuples = spark.sql("")

val tuplesToJson = orders
    .toJSON.rdd.repartition(10).toDF("body")
      .withColumn("partitionKey", get_json_object(col("body"),"$.id"))
    .cache();

tuplesToJson.write
  .format("eventhubs")
  .options(ehConf.toMap)
  .save() 
