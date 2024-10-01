package self.training.io

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import self.training.schemas.dataSchemas.{dev, enriched_data, run_env}

import java.util.Properties

object outputUtils {
  def writeEnrichedData(properties: Properties, dataset: Dataset[enriched_data], mode: run_env, processing_time: String): Unit = {
    if (mode == dev) {
      writeToConsole(dataset)
    } else {
      dataset.foreach(record => {
        record.target match {
          case kafka => writeToKafka(record, "2 seconds", "append", properties.getProperty("kafka.output.topic"))
          case console => writeToConsole(record)
          case mysql => writeToMySQL(record, "append", properties.getProperty("mysql.decline.table"), properties)
        }
      })
    }
  }

  def writeAnalyzedData[T](properties: Properties, dataset: Dataset[T], mode: run_env, processing_time: String, table: String): Unit = {
    if (mode == dev) {
      dataset.writeStream
        .foreachBatch { (batchDF, batchId) => {
          writeToMySQL(batchDF, "update", table, properties)
        }}
          .outputMode(OutputMode.Update())
          .format("console")
          .option("truncate", "false")
          .trigger(Trigger.ProcessingTime(processing_time))
          .start().awaitTermination()
        }
      else
      {
        dataset.writeStream
          .outputMode(OutputMode.Update())
          .format("kafka")
          .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
          .option("topic", properties.getProperty("kafka.output.topic.enriched"))
          .option("checkpointLocation", "checkpoint_dir") // Required for exactly-once guarantees
          .trigger(Trigger.ProcessingTime(processing_time))
          .start().awaitTermination()
      }
    }

  private def writeToMySQL[T](df: Dataset[T], saveMode: String, table: String, properties: Properties): Unit = {
    val connectionProperties: Properties = new Properties()
    connectionProperties.put("user", properties.getProperty("mysql.user"))
    connectionProperties.put("password", properties.getProperty("mysql.password"))

    df.write
      .mode(saveMode)
      .jdbc(properties.getProperty("mysql.connection.url"), table: String, connectionProperties)
  }

  private def writeToKafka[T](df: Dataset[T], processing_time: String, saveMode: String, properties: Properties): Unit = {
    df.writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
      .option("topic", properties.getProperty("kafka.output.topic.enriched"))
      .option("checkpointLocation", "checkpoint_dir") // Required for exactly-once guarantees
      .trigger(Trigger.ProcessingTime(processing_time))
      .start().awaitTermination()
  }

  private def writeToConsole[T](df: Dataset[T]): Unit = {
    df.write.format("console").save()
  }
}