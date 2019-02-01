package com.aravinth.spark.dataframe_operations

import com.aravinth.spark.utils.WithSpark
import org.apache.log4j.{Level, Logger}

object Operations extends App with WithSpark {


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

  dfTags.show(10)

  /* output
  +---+-------------------+
  | id|                tag|
  +---+-------------------+
  |  1|               data|
  |  4|                 c#|
  |  4|           winforms|
  |  4|    type-conversion|
  |  4|            decimal|
  |  4|            opacity|
  |  6|               html|
  |  6|                css|
  |  6|               css3|
  |  6|internet-explorer-7|
  +---+-------------------+
  only showing top 10 rows*/

  // Print DataFrame schema
  dfTags.printSchema()

  /* schema output
root
 |-- id: integer (nullable = true)
 |-- tag: string (nullable = true)
*/


  // Query dataframe: select columns from a dataframe
  dfTags.select("id", "tag").show(10)

  /* output
+---+-------------------+
| id|                tag|
+---+-------------------+
|  1|               data|
|  4|                 c#|
|  4|           winforms|
|  4|    type-conversion|
|  4|            decimal|
|  4|            opacity|
|  6|               html|
|  6|                css|
|  6|               css3|
|  6|internet-explorer-7|
+---+-------------------+*/

  // DataFrame Query: filter by column value of a dataframe
  dfTags.filter("tag == 'php'").show(10)

  /* output
 +---+---+
| id|tag|
+---+---+
| 23|php|
| 42|php|
| 85|php|
|126|php|
|146|php|
|227|php|
|249|php|
|328|php|
|588|php|
|657|php|
+---+---+*/

  // DataFrame Query: count rows of a dataframe
  println(s"Number of php tags = ${dfTags.filter("tag == 'php'").count()}")

  /*Number of php tags = 133*/

  // DataFrame Query: SQL like query
  dfTags.filter("tag like 's%'").show(10)

  /*
+---+-------------+
| id|          tag|
+---+-------------+
| 25|      sockets|
| 36|          sql|
| 36|   sql-server|
| 40| structuremap|
| 48|submit-button|
| 79|          svn|
| 79|    subclipse|
| 85|          sql|
| 90|          svn|
|108|          svn|
+---+-------------+*/

  // DataFrame Query: Multiple filter chaining
  dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)


  // DataFrame Query: SQL IN clause
  dfTags.filter("id in (25, 108)").show(10)


  println("Group by tag value")
  dfTags.groupBy("tag").count().show(10)


  // DataFrame Query: SQL Group By with filter
  dfTags.groupBy("tag").count().filter("count > 5").show(10)


  // DataFrame Query: SQL order by
  dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").show(10)


  // DataFrame Query: Cast columns to specific data type
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  dfQuestionsCSV.printSchema()


  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  dfQuestions.printSchema()
  dfQuestions.show(10)


  // DataFrame Query: Operate on a sliced dataframe
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()
  dfQuestionsSubset.show()


  // DataFrame Query: Join
  dfQuestionsSubset.join(dfTags, "id").show(10)

  // DataFrame Query: Join and select columns
  dfQuestionsSubset
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .show(10)

  // DataFrame Query: Join on explicit columns
  dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
    .show(10)

  // DataFrame Query: Inner Join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)

  // DataFrame Query: Left Outer Join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "left_outer")
    .show(10)

  // DataFrame Query: Right Outer Join
  dfTags
    .join(dfQuestionsSubset, Seq("id"), "right_outer")
    .show(10)

  // DataFrame Query: Distinct
  dfTags
    .select("tag")
    .distinct()
    .show(10)






}
