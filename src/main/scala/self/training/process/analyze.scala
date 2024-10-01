package self.training.process

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.Dataset
import self.training.schemas.dataSchemas.{amount_declined_per_account_type, amount_declined_per_decline_reason, enriched_data}

object analyze {
  def amountDeclinedPerCustomerTypeAnalysis(dataset: Dataset[enriched_data], timeframe: String):Dataset[amount_declined_per_account_type] = {
    import dataset.sparkSession.implicits._
    dataset.select("account_type", "amount", "time")
      .groupBy(
        window($"time", timeframe, "1 second"),
        $"account_type"
      )
      .sum("amount").as("declined_amount_per_account_type_per_timeframe")
      .select($"window.start".as("start_time"), $"window.end".as("end_time"), $"account_type", $"declined_amount_per_account_type_per_timeframe.sum(amount)".as("amount"))
      .as[amount_declined_per_account_type]
  }

  def amountDeclinedPerDeclineCodeAnalysis(dataset: Dataset[enriched_data], timeframe: String):Dataset[amount_declined_per_decline_reason] = {
    import dataset.sparkSession.implicits._
    dataset.select($"decline_reason", $"amount", $"time")
      .groupBy(
        window($"time", timeframe, "1 second"),
        $"decline_reason"
      )
      .sum("amount").as("total_amount_per_decline_reason_per_timeframe")
      .select($"window.start".as("start_time"), $"window.end".as("end_time"), $"decline_reason", $"total_amount_per_decline_reason_per_timeframe.sum(amount)".as("amount"))
      .as[amount_declined_per_decline_reason]
  }
}
