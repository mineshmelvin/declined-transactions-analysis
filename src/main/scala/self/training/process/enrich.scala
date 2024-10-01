package self.training.process

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.udf
import self.training.schemas.dataSchemas
import self.training.schemas.dataSchemas.{enriched_data, transaction}

object enrich {

  /**
   * Enriches the transaction data by adding customer information and rules from the customer and rule datasets using common account_no
   * @param transaction dataset of transactions
   * @param customer dataset of customers
   * @param rule dataset of rules
   * @return dataset[enriched_data]
   */
  def enrich(
              transaction: Dataset[transaction],
              customer: Broadcast[Map[Int, dataSchemas.customer]],
              rule: Broadcast[Map[Int, dataSchemas.rule]]
            ): Dataset[enriched_data] = {

    import transaction.sparkSession.implicits._

    val getCustomerUDF = udf((account_no: Int) =>
      customer.value.get(account_no))

    val getDeclineReasonUDF = udf((decline_code: Int) =>
      rule.value.get(decline_code)
    )

    transaction
      .withColumn("customer", getCustomerUDF($"account_no"))
      .withColumn("decline_info", getDeclineReasonUDF($"decline_code"))
      .select($"customer.id",
        $"customer.name",
        $"customer.account_type",
        $"customer.account_no",
        $"customer.card_no",
        $"customer.email",
        $"customer.phone",
        $"amount",
        $"merchant_id",
        $"status",
        $"decline_code",
        $"decline_info.decline_reason",
        $"time").as[enriched_data]
  }
}
