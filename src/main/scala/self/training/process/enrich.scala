package self.training.process

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.udf
import self.training.schemas.dataSchemas
import self.training.schemas.dataSchemas.{enriched_data, transaction}

object enrich {
  def enrich(
              transaction: Dataset[transaction],
              customer: Broadcast[Map[Int, dataSchemas.customer]],
              decline: Broadcast[Map[Int, dataSchemas.rule]]
            ): Dataset[enriched_data] = {

    import transaction.sparkSession.implicits._

    val getCustomerUDF = udf((account_no: Int) =>
      customer.value.get(account_no))

    val getDeclineReasonUDF = udf((decline_code: Int) =>
      decline.value.get(decline_code)
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
