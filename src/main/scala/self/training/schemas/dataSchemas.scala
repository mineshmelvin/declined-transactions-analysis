package self.training.schemas

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import self.training.schemas.dataSchemas.TargetType.TargetType
import self.training.schemas.dataSchemas.AccountType.AccountType
import self.training.schemas.dataSchemas.run_env.run_env

/**
 * Contains all the schemas required in the application to make it Strongly Typed
 */
object dataSchemas {

  implicit val accountTypeEncoder: Encoder[AccountType] = Encoders.kryo[AccountType]
  implicit val targetTypeEncoder: Encoder[TargetType] = Encoders.kryo[TargetType]
  implicit val run_envEncoder: Encoder[run_env] = Encoders.kryo[run_env]
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
                   account_type: AccountType,
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
                            account_type: AccountType,
                            account_no: Int,
                            card_no: Int,
                            email: String,
                            phone: String,
                            amount: Double,
                            merchant_id: Int,
                            status: String,
                            decline_code: Int,
                            decline_reason: String,
                            time: String,
                            target: TargetType
                          )

  case class amount_declined_per_account_type(
                                               start_time: String,
                                               end_time: String,
                                               account_type: AccountType,
                                               amount: Double
                                             )

  case class amount_declined_per_decline_reason(
                                                 start_time: String,
                                                 end_time: String,
                                                 decline_reason: String,
                                                 amount: Double
                                               )

  val transactionSchema: StructType = new StructType()
    .add("card_no", IntegerType)
    .add("account_no", IntegerType)
    .add("amount", DoubleType)
    .add("status", StringType)
    .add("decline_code", IntegerType)
    .add("merchant_id", IntegerType)
    .add("time", StringType)

//  sealed trait AccountType
//  case object VIP extends AccountType
//  case object retail extends AccountType
//  case object corporate extends AccountType

//  sealed trait run_env
//  case object dev extends run_env
//  case object prod extends run_env

//  sealed trait TargetType
//  case object kafka extends TargetType
//  case object mysql extends TargetType
//  case object console extends TargetType

  object TargetType extends Enumeration {
    type TargetType = Value
    val kafka, mysql, console = Value
  }

  object AccountType extends Enumeration {
    type AccountType = Value
    val retail, VIP, corporate = Value
  }

  object run_env extends Enumeration {
    type run_env = Value
    val prod, dev = Value
  }

  object AccountTypeEncoders {
    implicit def accountTypeEncoder: org.apache.spark.sql.Encoder[AccountType] =
      org.apache.spark.sql.Encoders.kryo[AccountType]
  }
}