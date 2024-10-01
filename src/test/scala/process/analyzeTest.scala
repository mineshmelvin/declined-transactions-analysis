package process

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import self.training.process.analyze.amountDeclinedPerAccountTypeAnalysis
import self.training.schemas.dataSchemas.{amount_declined_per_account_type, enriched_data}
import self.training.schemas.dataSchemas.TargetType.{kafka, console}
import self.training.schemas.dataSchemas.AccountType.VIP

class analyzeTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "amountDeclinedPerAccountTypeAnalysis" should "correctly analyze total declined amount per account type" in {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Analyze Test")
      .getOrCreate()

    import spark.implicits._
    import self.training.schemas.dataSchemas.AccountTypeEncoders._

    //Sample enriched_data
    val sampleData = Seq(
      enriched_data(1, "Minesh", VIP, 1234, 1111, "minesh@gmail.com", "6345223554", 100.0, 123, "declined", 101, "invalid_pin", "2023-10-01 23:06:02", kafka),
      enriched_data(2, "Minesh", VIP, 1234, 1111, "minesh@gmail.com", "6345223554", 200.0, 123, "declined", 102, "not_enough_balance", "2023-10-01 23:06:02", console)
    ).toDS()

    val timeframe = "10 seconds"
    val result: Dataset[amount_declined_per_account_type] = amountDeclinedPerAccountTypeAnalysis(sampleData, timeframe)

    result.collect() should contain theSameElementsAs Seq(
      amount_declined_per_account_type("2023-10-01 23:06:02", "2023-10-01 23:06:04", VIP, 300.0)
    )
    spark.stop()
  }
}