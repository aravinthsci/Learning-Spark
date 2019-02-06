package com.aravinth.spark.dataframe_operations

import org.apache.spark.sql.functions._
import com.aravinth.spark.utils.WithSpark
import org.apache.log4j.{Level, Logger}


object DataFrameStatistics extends App with WithSpark{

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  // Create a dataframe from questions file questions_10K.csv
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  // cast columns to data types
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  // Average
  dfQuestions
    .select(avg("score"))
    .show()
  /*output
+-----------------+
|       avg(score)|
+-----------------+
|36.14631463146315|
+-----------------+ */

  // Max
  dfQuestions
    .select(max("score"))
    .show()

  /*
  +----------+
|max(score)|
+----------+
|      4443|
+----------+
*/

  // Minimum
  dfQuestions
    .select(min("score"))
    .show()

  /*
+----------+
|min(score)|
+----------+
|       -27|
+----------+*/

  // Mean
  dfQuestions
    .select(mean("score"))
    .show()

  /*
  +-----------------+
|       avg(score)|
+-----------------+
|36.14631463146315|
+-----------------+*/

  // Sum
  dfQuestions
    .select(sum("score"))
    .show()

  /*
 +----------+
|sum(score)|
+----------+
|    361427|
+----------+
*/

  // Group by with statistics
  dfQuestions
    .filter("id > 400 and id < 450")
    .filter("owner_userid is not null")
    .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
    .groupBy(dfQuestions.col("owner_userid"))
    .agg(avg("score"), max("answer_count"))
    .show()

  /*
  +------------+----------+-----------------+
|owner_userid|avg(score)|max(answer_count)|
+------------+----------+-----------------+
|         268|      26.0|                1|
|         136|      57.6|                9|
|         123|      20.0|                3|
+------------+----------+-----------------+*/

  // DataFrame Statistics using describe() method
  val dfQuestionsStatistics = dfQuestions.describe()
  dfQuestionsStatistics.show()

  /*+-------+-----------------+------------------+-----------------+------------------+
|summary|               id|             score|     owner_userid|      answer_count|
+-------+-----------------+------------------+-----------------+------------------+
|  count|             9999|              9999|             7388|              9922|
|   mean|33929.17081708171| 36.14631463146315|47389.99472116947|6.6232614392259626|
| stddev|19110.09560532429|160.48316753972045|280943.1070344427| 9.069109116851138|
|    min|                1|               -27|                1|                -5|
|    max|            66037|              4443|          3431280|               316|
+-------+-----------------+------------------+-----------------+------------------+*/

  // Correlation
  val correlation = dfQuestions.stat.corr("score", "answer_count")
  println(s"correlation between column score and answer_count = $correlation")

  /*correlation between column score and answer_count = 0.3699847903294707*/

  // Covariance
  val covariance = dfQuestions.stat.cov("score", "answer_count")
  println(s"covariance between column score and answer_count = $covariance")

  /*covariance between column score and answer_count = 537.513381444165*/

  // Frequent Items
  val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
  dfFrequentScore.show()
  /*
  * +----------------------+
|answer_count_freqItems|
+----------------------+
|  [23, 131, 77, 86,...|
+----------------------+
*/

  // Crosstab
  val dfScoreByUserid = dfQuestions
    .filter("owner_userid > 0 and owner_userid < 20")
    .stat
    .crosstab("score", "owner_userid")
  dfScoreByUserid.show(10)

  /*
  +------------------+---+---+---+---+---+---+---+---+---+---+
|score_owner_userid|  1| 11| 13| 17|  2|  3|  4|  5|  8|  9|
+------------------+---+---+---+---+---+---+---+---+---+---+
|                56|  0|  0|  0|  1|  0|  0|  0|  0|  0|  0|
|               472|  0|  0|  0|  0|  0|  0|  0|  0|  1|  0|
|                14|  0|  0|  0|  1|  0|  0|  0|  1|  0|  0|
|                20|  0|  0|  0|  0|  0|  0|  0|  1|  0|  0|
|               179|  0|  0|  0|  0|  0|  1|  0|  0|  0|  0|
|                84|  0|  0|  0|  0|  1|  0|  0|  0|  0|  0|
|               160|  0|  0|  1|  0|  0|  0|  0|  0|  0|  0|
|                21|  0|  0|  0|  0|  0|  0|  0|  1|  0|  0|
|                 9|  0|  0|  0|  0|  0|  0|  1|  1|  0|  0|
|                 2|  0|  0|  0|  0|  0|  0|  0|  1|  0|  1|
+------------------+---+---+---+---+---+---+---+---+---+---+*/

  // find all rows where answer_count in (5, 10, 20)
  val dfQuestionsByAnswerCount = dfQuestions
    .filter("owner_userid > 0")
    .filter("answer_count in (5, 10, 20)")

  // count how many rows match answer_count in (5, 10, 20)
  dfQuestionsByAnswerCount
    .groupBy("answer_count")
    .count()
    .show()

  /*
+------------+-----+
|answer_count|count|
+------------+-----+
|          20|   34|
|           5|  811|
|          10|  272|
+------------+-----+*/


  // Create a fraction map where we are only interested:
  // - 50% of the rows that have answer_count = 5
  // - 10% of the rows that have answer_count = 10
  // - 100% of the rows that have answer_count = 20
  // Note also that fractions should be in the range [0, 1]
  val fractionKeyMap = Map(5 -> 0.5, 10 -> 0.1, 20 -> 1.0)

  // Stratified sample using the fractionKeyMap.
  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count", fractionKeyMap, 7L)
    .groupBy("answer_count")
    .count()
    .show()

  /*
  +------------+-----+
|answer_count|count|
+------------+-----+
|          20|   34|
|           5|  400|
|          10|   26|
+------------+-----+ */

  // Note that changing the random seed will modify your sampling outcome. As an example, let's change the random seed to 37.
  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count", fractionKeyMap, 37L)
    .groupBy("answer_count")
    .count()
    .show()

  /*
  * +------------+-----+
|answer_count|count|
+------------+-----+
|          20|   34|
|           5|  388|
|          10|   25|
+------------+-----+*/


  // Approximate Quantile
  val quantiles = dfQuestions
    .stat
    .approxQuantile("score", Array(0, 0.5, 1), 0.25)
  println(s"Qauntiles segments = ${quantiles.toSeq}")

  /*Qauntiles segments = WrappedArray(-27.0, 2.0, 4443.0)*/




}
