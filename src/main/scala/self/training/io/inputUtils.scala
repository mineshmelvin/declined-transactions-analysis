package self.training.io

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import self.training.schemas.dataSchemas.AccountType.AccountType
import self.training.schemas.dataSchemas.TargetType.TargetType
import self.training.schemas.dataSchemas.{customer, rule, transaction, transactionSchema}

object inputUtils {

  /**
   * Reads transactions data from Kafka topic and returns a stream of Dataset[transaction]
   * @param spark SparkSession of the application
   * @param topic Kafka topic to read from
   * @param bootstrapServers Bootstrap servers of the target Kafka Cluster
   * @return Dataset[transaction] containing a stream of transactions
   */
  def readTransactionsFromKafka(spark: SparkSession, topic: String, bootstrapServers: String): Dataset[transaction] = {
    import spark.implicits._
    val df = spark.readStream.format("kafka")
      .option("bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", transactionSchema).as("data"))
      .select("data.*")
      .as[transaction]
    df
  }

  /**
   * Reads customer data from Impala table and returns Dataset[customer]
   *
   * @param spark            SparkSession of the application
   * @param table            Table to read from
   * @return Dataset[customer] containing a dataset of customers
   */
  def readCustomerFromImpala(spark:SparkSession, table: String): Dataset[customer] = {
    val df = spark.read.table(table).select("account_no", "customer_name", "email", "phone", "customer_type")
    import spark.implicits._
    df.map(row => {
      customer(
        id = row.getAs[Int]("id"),
        name = row.getAs[String]("name"),
        phone = row.getAs[String]("phone"),
        email = row.getAs[String]("email"),
        account_type = row.getAs[String]("account_type"),
        account_no = row.getAs[Int]("account_no"),
        card_no = row.getAs[Int]("card_no")
      )
    })(Encoders.product[customer])
  }

  /**
   * Reads rules from Impala table and returns Dataset[rule]
   *
   * @param spark            SparkSession of the application
   * @param table            Impala table to read from
   * @return Dataset[rule] containing a dataset of rules
   */
  def readRuleFromImpala(spark: SparkSession, table:String): Dataset[rule] = {
    val df = spark.read.table(table).select("rule_id", "rule_desc", "account_type", "target", "decline_code","decline_reason")
    import spark.implicits._
    df.map(row => {
      rule(
        rule_id = row.getAs[Int]("rule_id"),
        rule_desc = row.getAs[String]("rule_desc"),
        account_type = row.getAs[AccountType]("account_type"),
        target = row.getAs[TargetType]("target"),
        decline_code = row.getAs[Int]("decline_code"),
        decline_reason = row.getAs[String]("decline_reason")
      )
    })(Encoders.product[rule])
  }
}