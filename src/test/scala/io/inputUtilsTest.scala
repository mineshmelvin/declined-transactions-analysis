package io

import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.from_json
import org.junit.Test
import self.training.schemas.dataSchemas.{transaction, transactionSchema}

class inputUtilsTest {

  implicit val encoder:Encoder[transaction] = ExpressionEncoder[transaction]

  //Create a SparkSession for testing
  val spark: SparkSession = SparkSession.builder().master("local").appName("inputUtilsTest").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  @Test
  def readTransactionFromKafkaTest(): Unit = {
    //create MemoryStream query to simulate kafka data stream

    val query = readTransactionFromKafkaSim("transactions")

    //Read the output from in-memory table
    val result = spark.sql("SELECT * FROM transactions")
      //.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", transactionSchema).as("data"))
      .select("data.*")
      .as[transaction]

    //perform assertions
    assert(result.count() == 2)
    assert(result.filter($"card_no" === 1234).head().amount == 100.0)
    assert(result.filter($"card_no" === 3422).head().status == "declined")

    query.stop()
  }

  private def readTransactionFromKafkaSim(queryName: String): StreamingQuery ={
    val memoryStream: MemoryStream[String] = new MemoryStream[String](1, spark.sqlContext)

    // Create a dataset representing the transactions in JSON format
    val testInput = Seq(
      """{"card_no": 1234, "account_no": 1111, "amount": 100.0, "status": "declined", "decline_code": 1, "merchant_id": 32, "time": "2023-10-01 21:31:22"}""",
      """{"card_no": 3422, "account_no": 2222, "amount": 500.0, "status": "declined", "decline_code": 2, "merchant_id": 52, "time": "2023-10-01 21:31:23"}"""
    )
    memoryStream.addData(testInput: _*)

    val query: StreamingQuery = memoryStream.toDF().writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("append")
      .start()

    query.processAllAvailable()
    query
  }


}