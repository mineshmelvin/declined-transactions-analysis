package self.training.io

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import self.training.schemas.dataSchemas.{CustomerType, TargetType, VIP, corporate, customer, dev, enriched_data, retail, rule, run_env, transaction, transactionSchema}

import java.util.Properties

object inputUtils {

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

  def readCustomerFromImpala(spark:SparkSession, table: String): Dataset[customer] = {
    val df = spark.read.table(table).select("account_no", "customer_name", "email", "phone", "customer_type")
    import spark.implicits._
    df.map(row => {
//      val customerTypeStr = row.getAs[String]("customer_type")
//      val customerTyped = customerTypeStr match {
//        case "VIP" => VIP
//        case "retail" => retail
//        case "corporate" => corporate
//      }

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

  def readRuleFromImpala(spark: SparkSession, table:String): Dataset[rule] = {
    val df = spark.read.table(table).select("rule_id", "rule_desc", "account_type", "target", "decline_code","decline_reason")
    import spark.implicits._
    df.map(row => {
      rule(
        rule_id = row.getAs[Int]("rule_id"),
        rule_desc = row.getAs[String]("rule_desc"),
        account_type = row.getAs[CustomerType]("account_type"),
        target = row.getAs[TargetType]("target"),
        decline_code = row.getAs[Int]("decline_code"),
        decline_reason = row.getAs[String]("decline_reason")
      )
    })(Encoders.product[rule])
  }
}