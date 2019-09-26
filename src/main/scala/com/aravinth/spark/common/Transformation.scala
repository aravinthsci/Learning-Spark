package com.aravinth.spark.common

import com.aravinth.spark.utils.WithSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Column}

case class Transaction(details: String, amount: Int, ts: Timestamp)


object Transformation extends App with WithSpark{

def sumAmounts(df: DataFrame, by: Column*): DataFrame =
    df.groupBy(by: _*).agg(sum(col("amount")))

  def extractPayerBeneficiary(columnName: String, df: DataFrame): DataFrame =
    df.withColumn(
      s"${columnName}_payer",
      regexp_extract(
        col(columnName),
        "paid by ([A-Z])",
        1
      )
    ).withColumn(
      s"${columnName}_beneficiary",
      regexp_extract(
        col(columnName),
        "to ([A-Z])",
        1
      )
    )

  type Transform = DataFrame => DataFrame

  def sumAmounts(by: Column*): Transform =
    df => df.groupBy(by: _*).agg(sum(col("amount")))

  def extractPayerBeneficiary(columnName: String): Transform =
    df =>
      df.withColumn(
        s"${columnName}_payer",
        regexp_extract(
          col(columnName),
          "paid by ([A-Z])", 1)
      ).withColumn(
        s"${columnName}_beneficiary",
        regexp_extract(
          col(columnName),
          "to ([A-Z])", 1))

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  import sparkSession.implicits._


  val dfTransactions: DataFrame = Seq(
    Transaction("paid by A to X", 100, Timestamp.valueOf("2018-01-05 08:00:00")),
    Transaction("paid by B to X", 10, Timestamp.valueOf("2018-01-05 11:00:00")),
    Transaction("paid by C to Y", 15, Timestamp.valueOf("2018-01-06 12:00:00")),
    Transaction("paid by D to Z", 50, Timestamp.valueOf("2018-01-06 15:00:00"))
  ).toDF

 /* dfTransactions
    .transform(extractPayerBeneficiary("details", _))
    .transform(sumAmounts(_, date_trunc("day", col("ts")), col("details_beneficiary")))
    .filter(col("sum(amount)") > 25)
    .show*/

  dfTransactions
    .transform(extractPayerBeneficiary("details"))
    .transform(sumAmounts(date_trunc("day", col("ts")), col("details_beneficiary"))).show()


  dfTransactions
    .transform(extractPayerBeneficiary("details") andThen sumAmounts(date_trunc("day", col("ts")), col("details_beneficiary"))).show()

  // compose
  dfTransactions
    .transform(sumAmounts(date_trunc("day", col("ts")), col("details_beneficiary")) compose extractPayerBeneficiary("details")).show()

  // Function.chain
  dfTransactions
    .transform(Function.chain(List(extractPayerBeneficiary("details"), sumAmounts(date_trunc("day", col("ts")), col("details_beneficiary")))))
    .show()



}


