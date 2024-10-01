package self.training.data

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import self.training.schemas.dataSchemas.{customer, rule, transaction}
import self.training.schemas.dataSchemas.TargetType.{kafka, console, mysql}
import self.training.schemas.dataSchemas.AccountType.{VIP, corporate, retail}

import java.time.Instant
import scala.util.Random

/**This object contains functions and hardcoded customer information to simulate generation of customers,
transactions, and rules for testing purpose */

object sample_data {
  /**
   *
   * @param spark: SparkSession to  create a Dataset
   * @return Dataset[customer] of randomly generated customers for testing purpose
   */
  def generate_customer_data(spark: SparkSession):Dataset[customer] = {
    import spark.implicits._
    customers.toDS()
  }

  /**
   *
   * @param spark : SparkSession to  create a Dataset
   * @return Dataset[rule] of randomly generated rules for testing purpose
   */

  def generate_rules(spark: SparkSession): Dataset[rule] = {
    import spark.implicits._
    rules.toDS()
  }

  /**
   *
   * @param spark : SparkSession to  create a Dataset
   * @param numTransaction: the number of transactions to be produced
   * @return Dataset[customer] of randomly generated transactions for testing purpose
   */

  def generate_transaction_data(spark: SparkSession, numTransaction: Int): Dataset[transaction] = {
    import spark.implicits._
    implicit val sqlContext: SQLContext = spark.sqlContext

    val transactionStream = new MemoryStream[transaction](1, sqlContext)

    def randomTransaction(): transaction = {
      val randomCustomer = customers(Random.nextInt(customers.length))
      val randomDecline = rules(Random.nextInt(rules.length))
      generate_random_transaction(randomCustomer, randomDecline)
    }

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val newTransactions = (1 to numTransaction).map(_ => randomTransaction())
        transactionStream.addData(newTransactions)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })
    transactionStream.toDS().as[transaction]
  }

  // Hardcoded sample customers
  private val customers: Seq[customer] = Seq(
    customer(1, "Minesh", "6345223554", "minesh@gmail.com", "VIP", 1234, 1111),
    customer(2, "Fuedal", "2435535422", "fuedal@gmail.com", "VIP", 2345, 2222),
    customer(3, "Bibin", "8765773829", "bibinmark@gmail.com", "retail", 4532, 3333),
    customer(4, "Nishil", "6573293030", "nishilstephan@gmail.com", "corporate", 3456, 4444),
    customer(5, "Rocco", "9876492920", "rocco@gmail.com", "retail", 4674, 5555),
    customer(6, "Sanoop", "8573904743", "sanoop@gmail.com", "corporate", 5254, 6666),
    customer(7, "Frennie", "7480282746", "frennie@gmail.com", "retail", 6795, 7777),
    customer(8, "Syed", "8274664820", "syed@gmail.com", "corporate", 3662, 8888),
    customer(9, "Sadiq", "5362890200", "sadiq@gmail.com", "retail", 2355, 9999)
  )

  // Hardcoded sample rules
  private val rules: Seq[rule] = Seq(
    rule(1, "write retail transactions with decline_code 101 to kafka", retail, kafka, 101, "invalid_pin"),
    rule(2, "write VIP transactions with decline_code 101 to MySQL", VIP, mysql, 101, "invalid_pin"),
    rule(3, "write corporate transactions with decline_code 101 to console", corporate, console, 101, "invalid_pin"),
    rule(4, "write retail transactions with decline_code 102 to MySQL", retail, mysql, 102, "not_enough_balance"),
    rule(5, "write corporate transactions with decline_code 102 to console", corporate, kafka, 102, "not_enough_balance"),
    rule(6, "write VIP transactions with decline_code 102 to console", VIP, console, 102, "not_enough_balance"),
    rule(7, "write retail transactions with decline_code 103 to kafka", retail, mysql, 103, "network_issue"),
    rule(8, "write VIP transactions with decline_code 103 to kafka", VIP, kafka, 103, "network_issue"),
    rule(9, "write corporate transactions with decline_code 103 to kafka", corporate, console, 103, "network_issue")
  )

  private def generate_random_transaction(customer: customer, declines: rule):transaction = {
    val amount = Random.nextFloat() * 1000
    val merchant_id = Random.nextInt(10)
    val timestamp = Instant.now.toString
    transaction(customer.card_no, customer.account_no, amount, "declined", declines.decline_code, merchant_id, timestamp)
  }
}
