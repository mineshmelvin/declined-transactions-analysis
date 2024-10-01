package self.training.io

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import self.training.schemas.dataSchemas.enriched_data
import self.training.schemas.dataSchemas.run_env.{dev, prod, run_env}

import java.util.Properties

object outputUtils {
  /**
   * (Still figuring out if I should write to sink as individual record or dataset)
   * Writes enriched data to downstream targets depending on the target column
   * @param properties java.util.Properties object containing the configurations of the application
   * @param dataset[enriched_data]  to dataset to be written to downstream sink
   * @param mode if the application is running in dev or prod mode (dev writes all data to console, and prod writes to respective sinks based on target column of dataset)
   */
  def writeEnrichedData(properties: Properties, dataset: Dataset[enriched_data], mode: run_env): Unit = {
    if (mode == dev) {
      writeToConsole(dataset)
    } else {
      dataset.foreach(record => {
        record.target match {
          case kafka => println("Something")//writeToKafka(record, "2 seconds", properties.getProperty("kafka.output.topic"), properties.getProperty("kafka.output.topic.enriched"))
          //case console => writeToConsole(record)
          //case mysql => writeToMySQL(record, "append", properties.getProperty("mysql.decline.table"), properties)
        }
      })
    }
  }

  /**
   * Not yet implemented
   * @param properties
   * @param dataset
   * @param mode
   * @param processing_time
   * @param table
   * @tparam T
   */
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

  /**
   * Writes the analysis results data to MySQL table
   *
   * @param df dataset to be written to MySQL table
   * @param saveMode spark write mode to follow: overwrite, append, ignore, or error as string
   * @param table the table to which the dataset has to be written
   * @param properties java.util.Properties object containing the configurations of the application
   */
  private def writeToMySQL[T](df: Dataset[T], saveMode: String, table: String, properties: Properties): Unit = {
    val connectionProperties: Properties = new Properties()
    connectionProperties.put("user", properties.getProperty("mysql.user"))
    connectionProperties.put("password", properties.getProperty("mysql.password"))

    df.write
      .mode(saveMode)
      .jdbc(properties.getProperty("mysql.connection.url"), table: String, connectionProperties)
  }

  /**
   * Writes a dataset to Kafka topic
   *
   * @param df         dataset to be written to Kafka topic
   * @param processing_time  Trigger processing time in string ex: "2 seconds"
   * @param properties java.util.Properties object containing the configurations of the application
   * @param topic name of the kafka topic to write to
   */
  private def writeToKafka[T](df: Dataset[T], processing_time: String, properties: Properties, topic: String): Unit = {
    df.writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
      .option("topic", topic)
      .option("checkpointLocation", "checkpoint_dir") // Required for exactly-once guarantees
      .trigger(Trigger.ProcessingTime(processing_time))
      .start().awaitTermination()
  }

  /**
   * Write a df to the console
   * @param df the dataset to write on the console
   */
  private def writeToConsole[T](df: Dataset[T]): Unit = {
    df.write.format("console").save()
  }
}