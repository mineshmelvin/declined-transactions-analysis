package self.training.schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object dataSchemas {
  case class transaction(
                             card_no: Int,
                             account_no: Int,
                             amount: Double,
                             status: String,
                             decline_code: Int,
                             merchant_id: Int,
                             time: String
                           )

  case class rule(
                   rule_id: Int,
                   rule_desc: String,
                   account_type: CustomerType,
                   target: TargetType,
                   decline_code: Int,
                   decline_reason: String
                   )

  case class customer(
                     id: Int,
                     name: String,
                     phone: String,
                     email: String,
                     account_type: String,
                     card_no: Int,
                     account_no: Int
                     )

  case class enriched_data(
                            id: Int,
                            name: String,
                            account_type: String,
                            account_no: String,
                            card_no: String,
                            email: String,
                            phone: String,
                            amount: Double,
                            merchant_id: Int,
                            status: String,
                            decline_code: Int,
                            decline_reason: String,
                            time: String,
                            target: String
                          )

  case class amount_declined_per_account_type(
                                               start_time: String,
                                               end_time: String,
                                               account_type: String,
                                               amount: Double
                                             )

  case class amount_declined_per_decline_reason(
                                                 start_time: String,
                                                 end_time: String,
                                                 decline_reason: String,
                                                 amount: Double
                                               )

  val transactionSchema: StructType = new StructType()
    .add("card_no", StringType)
    .add("card_no", IntegerType)
    .add("account_no", IntegerType)
    .add("amount", DoubleType)
    .add("status", StringType)
    .add("decline_code", IntegerType)
    .add("merchant_id", IntegerType)
    .add("time", StringType)

  val customerSchema = new StructType()
    .add("account_no", StringType)
    .add("customer_name", StringType)
    .add("email", StringType)
    .add("phone", StringType)
    .add("customer_type", StringType)

  sealed trait CustomerType
  case object VIP extends CustomerType
  case object retail extends CustomerType
  case object corporate extends CustomerType

  sealed trait run_env
  case object dev extends run_env
  case object prod extends run_env

  sealed trait TargetType
  case object kafka extends TargetType
  case object mysql extends TargetType
  case object console extends TargetType
}
