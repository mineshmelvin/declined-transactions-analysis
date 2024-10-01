package self.training

import org.apache.spark.sql.SparkSession
import self.training.config.propertiesLoader.loadProperties
import self.training.data.sample_data.{generate_customer_data, generate_rules, generate_transaction_data}
import self.training.io.inputUtils.{readCustomerFromImpala, readRuleFromImpala, readTransactionsFromKafka}
import self.training.process.enrich.enrich
import self.training.schemas.dataSchemas.{customer, rule, transaction}
import self.training.schemas.dataSchemas.run_env.{dev, prod}
import org.apache.spark.util.SizeEstimator
import self.training.io.outputUtils.{writeAnalyzedData, writeEnrichedData}
import self.training.process.analyze.{amountDeclinedPerAccountTypeAnalysis, amountDeclinedPerDeclineCodeAnalysis}

/**
 * @author ${Minesh.Melvin}
 */

object declined_transactions_analysis extends App {
  val spark = SparkSession.builder().master("local").appName("Declined Transactions Analysis").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val properties = loadProperties("/path/to/config/application.conf")

  private var transactionDS = spark.emptyDataset[transaction]
  private var customerDS = spark.emptyDataset[customer]
  private var ruleDS = spark.emptyDataset[rule]

  /**
   * Populate the datasets with random data if the runtime environment is dev else reads from respective sources
   */
  if (properties.getProperty("runtime.environment") == dev.toString) {
      transactionDS = generate_transaction_data(spark, 1000)
      customerDS = generate_customer_data(spark)
      ruleDS = generate_rules(spark)
  } else {
      transactionDS = readTransactionsFromKafka(
          spark,
          properties.getProperty("kafka.input.topic.transaction"),
          properties.getProperty("kafka.bootstrap.servers")
        )
      customerDS = readCustomerFromImpala(spark, properties.getProperty("impala.customer"))
      ruleDS = readRuleFromImpala(spark, properties.getProperty("impala.decline"))
    }
  println(SizeEstimator.estimate(customerDS))
  println(SizeEstimator.estimate(ruleDS))

  /**
   * Broadcast the small Datasets to make joins faster by avoiding shuffling
   */
  private val broadcastedCustomerDS = spark.sparkContext.broadcast(customerDS.collect().map(cust => (cust.account_no, cust)).toMap)
  private val broadcastedRuleDS = spark.sparkContext.broadcast(ruleDS.collect().map(dec => (dec.decline_code, dec)).toMap)

  //Perform enrichment
  private val enrichedDS = enrich(transactionDS, broadcastedCustomerDS, broadcastedRuleDS)

  //Perform analysis
  private val amountDeclinedPerCustomerType = amountDeclinedPerAccountTypeAnalysis(enrichedDS, "2 seconds")
  private val amountDeclinedPerDeclineCode = amountDeclinedPerDeclineCodeAnalysis(enrichedDS, "2 seconds")

  //Write Downstream
  writeEnrichedData(properties, enrichedDS, prod)
  writeAnalyzedData(properties, amountDeclinedPerCustomerType, prod, "2 seconds", properties.getProperty("mysql.amountDeclinedPerCustomerType.table"))
  writeAnalyzedData(properties, amountDeclinedPerDeclineCode, prod, "2 seconds", properties.getProperty("mysql.amountDeclinedPerDeclineCode.table"))
}