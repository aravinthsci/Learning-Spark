package com.aravinth.spark.spark_sql_operations

import com.aravinth.spark.utils.WithSpark
import org.apache.log4j.{Level, Logger}

object SparkSQL_Tutorial extends App with WithSpark {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Register temp table
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.createOrReplaceTempView("so_tags")
  dfTags.show()

  /*output
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
|  8|                 c#|
|  8|    code-generation|
|  8|                 j#|
|  8|           visualj#|
|  9|                 c#|
|  9|               .net|
|  9|           datetime|
| 11|                 c#|
| 11|           datetime|
| 11|           datediff|
+---+-------------------+
only showing top 20 rows*/

  // List all tables in Spark's catalog
  sparkSession.catalog.listTables().show()

  /*output
+-------+--------+-----------+---------+-----------+
|   name|database|description|tableType|isTemporary|
+-------+--------+-----------+---------+-----------+
|so_tags|    null|       null|TEMPORARY|       true|
+-------+--------+-----------+---------+-----------+*/

  // List all tables in Spark's catalog using Spark SQL
  sparkSession.sql("show tables").show()

  /* output
  +--------+---------+-----------+
|database|tableName|isTemporary|
+--------+---------+-----------+
|        |  so_tags|       true|
+--------+---------+-----------+*/

  // Select columns
  sparkSession
    .sql("select id, tag from so_tags limit 10")
    .show()

  /*output
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

  // Filter by column value
  sparkSession
    .sql("select * from so_tags where tag = 'php'")
    .show(10)

  /*
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

  // Count number of rows
  sparkSession
    .sql(
      """select
        |count(*) as php_count
        |from so_tags where tag='php'""".stripMargin)
    .show(10)

  /*output
 +---------+
|php_count|
+---------+
|      133|
+---------+*/

  // SQL like
  sparkSession
    .sql(
      """select *
        |from so_tags
        |where tag like 's%'""".stripMargin)
    .show(10)

  /*output
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

  // SQL where with and clause
  sparkSession
    .sql(
      """select *
        |from so_tags
        |where tag like 's%'
        |and (id = 25 or id = 108)""".stripMargin)
    .show(10)
  /*output
+---+-------+
| id|    tag|
+---+-------+
| 25|sockets|
|108|    svn|
+---+-------+*/

  // SQL IN clause
  sparkSession
    .sql(
      """select *
        |from so_tags
        |where id in (25, 108)""".stripMargin)
    .show(10)
/*output
* +---+---------+
| id|      tag|
+---+---------+
| 25|      c++|
| 25|        c|
| 25|  sockets|
| 25|mainframe|
| 25|      zos|
|108|  windows|
|108|      svn|
|108|    64bit|
+---+---------+
*/

  // SQL Group By
  sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag""".stripMargin)
    .show(10)

  /*output
  *+--------------------+-----+
|                 tag|count|
+--------------------+-----+
|         type-safety|    4|
|             jbutton|    1|
|              iframe|    2|
|           svn-hooks|    2|
|           standards|    7|
|knowledge-management|    2|
|            trayicon|    1|
|           arguments|    1|
|                 zfs|    1|
|              import|    3|
+--------------------+-----+ */


  // SQL Group By with having clause
  sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag having count > 5""".stripMargin)
    .show(10)

  /*output
  * +----------------+-----+
|             tag|count|
+----------------+-----+
|       standards|    7|
|        keyboard|    8|
|             rss|   12|
|   documentation|   15|
|         session|    6|
|build-automation|    9|
|            unix|   34|
|          iphone|   16|
|             xss|    6|
| database-design|   12|
+----------------+-----+*/

  // SQL Order by
    sparkSession
      .sql(
        """select tag, count(*) as count
          |from so_tags group by tag having count > 5 order by tag""".stripMargin)
      .show(10)

  /*output
  * +----------------+-----+
|             tag|count|
+----------------+-----+
|            .net|  351|
|        .net-2.0|   14|
|        .net-3.5|   30|
|           64bit|    7|
|  actionscript-3|   22|
|active-directory|   10|
|         ado.net|   11|
|           adobe|    7|
|           agile|    8|
|             air|   11|
+----------------+-----+
*/

  // Typed dataframe, filter and temp table
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

  // filter dataframe
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()

  // register temp table
  dfQuestionsSubset.createOrReplaceTempView("so_questions")

  // SQL Inner Join
  sparkSession
    .sql(
      """select t.*, q.*
        |from so_questions q
        |inner join so_tags t
        |on t.id = q.id""".stripMargin)
    .show(10)

  /*output
  *+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+
|  id|                 tag|  id|      creation_date|        closed_date|deletion_date|score|owner_userid|answer_count|
+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+
| 888|              xdebug| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|            phpstorm| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|           debugging| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|             eclipse| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|                 php| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
|1939|                 osx|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|                 ios|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|         objective-c|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|              iphone|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|3881|illegalargumentex...|3881|2008-08-07 00:56:30|2016-09-23 19:04:31|         null|  402|         122|          27|
+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+ */

  // SQL Left Outer Join
  sparkSession
    .sql(
      """select t.*, q.*
        |from so_questions q
        |left outer join so_tags t
        |on t.id = q.id""".stripMargin)
    .show(10)

  /*
+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+
|  id|                 tag|  id|      creation_date|        closed_date|deletion_date|score|owner_userid|answer_count|
+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+
| 888|              xdebug| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|            phpstorm| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|           debugging| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|             eclipse| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|                 php| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
|1939|                 osx|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|                 ios|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|         objective-c|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|              iphone|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|3881|illegalargumentex...|3881|2008-08-07 00:56:30|2016-09-23 19:04:31|         null|  402|         122|          27|
+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+*/

  // SQL Right Outer Join
  sparkSession
    .sql(
      """select t.*, q.*
        |from so_tags t
        |right outer join so_questions q
        |on t.id = q.id""".stripMargin)
    .show(10)

  /*output
  +----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+
|  id|                 tag|  id|      creation_date|        closed_date|deletion_date|score|owner_userid|answer_count|
+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+
| 888|              xdebug| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|            phpstorm| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|           debugging| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|             eclipse| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
| 888|                 php| 888|2008-08-04 04:48:21|2016-08-04 14:52:00|         null|  405|         131|          30|
|1939|                 osx|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|                 ios|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|         objective-c|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|1939|              iphone|1939|2008-08-05 11:09:36|2012-06-05 18:43:38|   2012-12-18|  408|        null|          48|
|3881|illegalargumentex...|3881|2008-08-07 00:56:30|2016-09-23 19:04:31|         null|  402|         122|          27|
+----+--------------------+----+-------------------+-------------------+-------------+-----+------------+------------+*/

  // SQL Distinct
  sparkSession
    .sql("""select distinct tag from so_tags""".stripMargin)
    .show(10)

  /*output
  +--------------------+
|                 tag|
+--------------------+
|         type-safety|
|             jbutton|
|              iframe|
|           svn-hooks|
|           standards|
|knowledge-management|
|            trayicon|
|           arguments|
|                 zfs|
|              import|
+--------------------+*/

  def prefixStackoverflow(s: String): String = s"so_$s"

  // Register User Defined Function (UDF)
  sparkSession
    .udf
    .register("prefix_so", prefixStackoverflow _)

  // Use udf prefix_so to augment each tag value with so_
  sparkSession
    .sql("""select id, prefix_so(tag) from so_tags""".stripMargin)
    .show(10)

  /*+---+--------------------+
| id|  UDF:prefix_so(tag)|
+---+--------------------+
|  1|             so_data|
|  4|               so_c#|
|  4|         so_winforms|
|  4|  so_type-conversion|
|  4|          so_decimal|
|  4|          so_opacity|
|  6|             so_html|
|  6|              so_css|
|  6|             so_css3|
|  6|so_internet-explo...|
+---+--------------------+
*/


}
