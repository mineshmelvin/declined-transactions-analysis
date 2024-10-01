package self.training

import org.apache.spark.sql.SparkSession
import self.training.config.propertiesLoader.loadProperties
import self.training.data.sample_data.{generate_customer_data, generate_rules, generate_transaction_data}
import self.training.io.inputUtils.{readCustomerFromImpala, readRuleFromImpala, readTransactionsFromKafka}
import self.training.process.enrich.enrich
import self.training.schemas.dataSchemas.{customer, dev, prod, rule, transaction}
import org.apache.spark.util.SizeEstimator
import self.training.io.outputUtils.{writeAnalyzedData, writeEnrichedData}
import self.training.process.analyze.{amountDeclinedPerCustomerTypeAnalysis, amountDeclinedPerDeclineCodeAnalysis}

/**
 * @author ${user.name}
 */

object declined_transactions_analysis extends App {
  val spark = SparkSession.builder().master("local").appName("radar").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val properties = loadProperties("C:\\Users\\mines\\workspace\\projects\\training\\radar_replica\\src\\main\\scala\\self\\training\\config\\application.conf")

  private var transactionDF = spark.emptyDataset[transaction]
  private var customerDF = spark.emptyDataset[customer]
  private var ruleDF = spark.emptyDataset[rule]

  if (properties.getProperty("runtime.environment") == dev.toString) {
      transactionDF = generate_transaction_data(spark, 1000)
      customerDF = generate_customer_data(spark)
      ruleDF = generate_rules(spark)
  } else {
      transactionDF = readTransactionsFromKafka(
          spark,
          properties.getProperty("kafka.input.topic.transaction"),
          properties.getProperty("kafka.bootstrap.servers")
        )
      customerDF = readCustomerFromImpala(spark, properties.getProperty("impala.customer"))
      ruleDF = readRuleFromImpala(spark, properties.getProperty("impala.decline"))
    }
  println(SizeEstimator.estimate(customerDF))
  println(SizeEstimator.estimate(ruleDF))

  private val broadcastedCustomerDF = spark.sparkContext.broadcast(customerDF.collect().map(cust => (cust.account_no, cust)).toMap)
  private val broadcastedRuleDF = spark.sparkContext.broadcast(ruleDF.collect().map(dec => (dec.decline_code, dec)).toMap)

  private val enrichedDF = enrich(transactionDF, broadcastedCustomerDF, broadcastedRuleDF)

  private val amountDeclinedPerCustomerType = amountDeclinedPerCustomerTypeAnalysis(enrichedDF, "2 seconds")
  private val amountDeclinedPerDeclineCode = amountDeclinedPerDeclineCodeAnalysis(enrichedDF, "2 seconds")

  writeEnrichedData(properties, enrichedDF, prod, "2 seconds")
  writeAnalyzedData(properties, amountDeclinedPerCustomerType, prod, "2 seconds", properties.getProperty("mysql.amountDeclinedPerCustomerType.table"))
  writeAnalyzedData(properties, amountDeclinedPerDeclineCode, prod, "2 seconds", properties.getProperty("mysql.amountDeclinedPerDeclineCode.table"))
}