package com.aravinth.spark.dataframe_operations

import com.aravinth.spark.utils.WithSpark
import org.apache.spark.sql.Dataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object DataFrameOperations extends App with WithSpark{

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

  /*
  * +---+-------------------+
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
*/

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", false)
    .option("inferSchema", true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score < 410")
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .toDF()

  dfQuestions.show(10)

  /*
  * +------------+--------------------+--------------------+-----+
|owner_userid|                 tag|       creation_date|score|
+------------+--------------------+--------------------+-----+
|         131|              xdebug|2008-08-03T23:18:21Z|  405|
|         131|            phpstorm|2008-08-03T23:18:21Z|  405|
|         131|           debugging|2008-08-03T23:18:21Z|  405|
|         131|             eclipse|2008-08-03T23:18:21Z|  405|
|         131|                 php|2008-08-03T23:18:21Z|  405|
|          NA|                 osx|2008-08-05T05:39:36Z|  408|
|          NA|                 ios|2008-08-05T05:39:36Z|  408|
|          NA|         objective-c|2008-08-05T05:39:36Z|  408|
|          NA|              iphone|2008-08-05T05:39:36Z|  408|
|         122|illegalargumentex...|2008-08-06T19:26:30Z|  402|
+------------+--------------------+--------------------+-----+
only showing top 10 rows*/

  case class Tag(id: Int, tag: String)

  import sparkSession.implicits._
  val dfTagsOfTag: Dataset[Tag] = dfTags.as[Tag]
  dfTagsOfTag
    .take(10)
    .foreach(t => println(s"id = ${t.id}, tag = ${t.tag}"))

  /*
  * id = 1, tag = data
id = 4, tag = c#
id = 4, tag = winforms
id = 4, tag = type-conversion
id = 4, tag = decimal
id = 4, tag = opacity
id = 6, tag = html
id = 6, tag = css
id = 6, tag = css3
id = 6, tag = internet-explorer-7*/


  case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int)
  // create a function which will parse each element in the row
  def toQuestion(row: org.apache.spark.sql.Row): Question = {
    // to normalize our owner_userid data
    val IntOf: String => Option[Int] = _ match {
      case s if s == "NA" => None
      case s => Some(s.toInt)
    }

    import java.time._
    val DateOf: String => java.sql.Timestamp = _ match {
      case s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)
    }

    Question (
      owner_userid = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      creationDate = DateOf(row.getString(2)),
      score = row.getString(3).toInt
    )
  }

  // now let's convert each row into a Question case class
  val dfOfQuestion: Dataset[Question] = dfQuestions.map(row => toQuestion(row))
  dfOfQuestion
    .take(10)
    .foreach(q => println(s"owner userid = ${q.owner_userid}, tag = ${q.tag}, creation date = ${q.creationDate}, score = ${q.score}"))

  /*
owner userid = 131, tag = xdebug, creation date = 2008-08-03 23:18:21.0, score = 405
owner userid = 131, tag = phpstorm, creation date = 2008-08-03 23:18:21.0, score = 405
owner userid = 131, tag = debugging, creation date = 2008-08-03 23:18:21.0, score = 405
owner userid = 131, tag = eclipse, creation date = 2008-08-03 23:18:21.0, score = 405
owner userid = 131, tag = php, creation date = 2008-08-03 23:18:21.0, score = 405
owner userid = -1, tag = osx, creation date = 2008-08-05 05:39:36.0, score = 408
owner userid = -1, tag = ios, creation date = 2008-08-05 05:39:36.0, score = 408
owner userid = -1, tag = objective-c, creation date = 2008-08-05 05:39:36.0, score = 408
owner userid = -1, tag = iphone, creation date = 2008-08-05 05:39:36.0, score = 408
owner userid = 122, tag = illegalargumentexception, creation date = 2008-08-06 19:26:30.0, score = 402*/

  //Create DataFrame from collection

  val seqTags = Seq(
    1 -> "so_java",
    1 -> "so_jsp",
    2 -> "so_erlang",
    3 -> "so_scala",
    3 -> "so_akka"
  )

  val dfMoreTags = seqTags.toDF("id", "tag")
  dfMoreTags.show(10)


  /*+---+---------+
| id|      tag|
+---+---------+
|  1|  so_java|
|  1|   so_jsp|
|  2|so_erlang|
|  3| so_scala|
|  3|  so_akka|
+---+---------+
*/


  //DataFrame Union

  val dfUnionOfTags = dfTags
    .union(dfMoreTags)
    .filter("id in (1,3)")
  dfUnionOfTags.show(10)

  /*
  * +---+--------+
| id|     tag|
+---+--------+
|  1|    data|
|  1| so_java|
|  1|  so_jsp|
|  3|so_scala|
|  3| so_akka|
+---+--------+*/


  //DataFrame Intersection

  val dfIntersectionTags = dfMoreTags
    .intersect(dfUnionOfTags)
    .show(10)

  /*+---+--------+
| id|     tag|
+---+--------+
|  3|so_scala|
|  3| so_akka|
|  1| so_java|
|  1|  so_jsp|
+---+--------+*/

  val dfSplitColumn = dfMoreTags
    .withColumn("tmp", split($"tag", "_"))
    .select(
      $"id",
      $"tag",
      $"tmp".getItem(0).as("so_prefix"),
      $"tmp".getItem(1).as("so_tag")
    ).drop("tmp")
  dfSplitColumn.show(10)

  /*+---+---------+---------+------+
| id|      tag|so_prefix|so_tag|
+---+---------+---------+------+
|  1|  so_java|       so|  java|
|  1|   so_jsp|       so|   jsp|
|  2|so_erlang|       so|erlang|
|  3| so_scala|       so| scala|
|  3|  so_akka|       so|  akka|
+---+---------+---------+------+*/

  //Create DataFrame from Tuples
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  df.show()

  /*
  * +-------------+-----+
|   Donut Name|Price|
+-------------+-----+
|  plain donut|  1.5|
|vanilla donut|  2.0|
| glazed donut|  2.5|
+-------------+-----+*/

  //Get DataFrame column names
  val columnNames: Array[String] = df.columns
  columnNames.foreach(name => println(s"$name"))

  /*
  * Donut Name
Price*/

  //DataFrame column names and types

  val (columnNames1, columnDataTypes) = df.dtypes.unzip
  println(s"DataFrame column names = ${columnNames1.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")

  /*
  * DataFrame column names = Donut Name, Price
DataFrame column data types = StringType, DoubleType*/

  //Json into DataFrame using explode()

  val tagsDF = sparkSession
    .read
    .option("multiLine", true)
    .option("inferSchema", true)
    .json("src/main/resources/tags_sample.json")

  var df_json = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags")

  df_json.printSchema()

  /*
  * root
 |-- stackoverflow_tags: struct (nullable = true)
 |    |-- tag: struct (nullable = true)
 |    |    |-- author: string (nullable = true)
 |    |    |-- frameworks: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- id: long (nullable = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- name: string (nullable = true)*/


  df_json = df_json.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  )

  df_json.show()
/*
*
* +---+--------------+--------+-------------+--------------------+
| id|        author|tag_name|frameworks_id|     frameworks_name|
+---+--------------+--------+-------------+--------------------+
|  1|Martin Odersky|   scala|       [1, 2]|[Play Framework, ...|
|  2| James Gosling|    java|       [1, 2]|[Apache Tomcat, S...|
+---+--------------+--------+-------------+--------------------+*/

  //Search DataFrame column using array_contains()
  df_json
    .select("*")
    .where(array_contains($"frameworks_name","Play Framework"))
    .show()

  /*
  * +---+--------------+--------+-------------+--------------------+
| id|        author|tag_name|frameworks_id|     frameworks_name|
+---+--------------+--------+-------------+--------------------+
|  1|Martin Odersky|   scala|       [1, 2]|[Play Framework, ...|
+---+--------------+--------+-------------+--------------------+*/

  //Concatenate DataFrames using join()

  val donut = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))

  val dfDonuts = sparkSession
    .createDataFrame(donut)
    .toDF("Id","Donut Name", "Price")
  dfDonuts.show()

  val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
  val dfInventory = sparkSession
    .createDataFrame(inventory)
    .toDF("Id", "Inventory")
  dfInventory.show()

  val dfDonutsInventory = dfDonuts.join(dfInventory, Seq("Id"), "inner")
  dfDonutsInventory.show()
/*+---+-------------+-----+---------+
| Id|   Donut Name|Price|Inventory|
+---+-------------+-----+---------+
|111|  plain donut|  1.5|       10|
|222|vanilla donut|  2.0|       20|
|333| glazed donut|  2.5|       30|
+---+-------------+-----+---------+
*/

//Check DataFrame column exists
  val priceColumnExists = dfDonuts.columns.contains("Price")
  println(s"Does price column exist = $priceColumnExists")

  /*Does price column exist = true*/


  //Split DataFrame Array column
  val targets = Seq(("Plain Donut", Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))
  val targetsdf = sparkSession
    .createDataFrame(targets)
    .toDF("Name", "Prices")

  targetsdf.show()

  /*
+----------------+----------+
|            Name|    Prices|
+----------------+----------+
|     Plain Donut|[1.5, 2.0]|
|   Vanilla Donut|[2.0, 2.5]|
|Strawberry Donut|[2.5, 3.5]|
+----------------+----------+
*/

  val df2 = targetsdf
    .select(
      $"Name",
      $"Prices"(0).as("Low Price"),
      $"Prices"(1).as("High Price")
    )

  df2.show()

  /*
+----------------+---------+----------+
|            Name|Low Price|High Price|
+----------------+---------+----------+
|     Plain Donut|      1.5|       2.0|
|   Vanilla Donut|      2.0|       2.5|
|Strawberry Donut|      2.5|       3.5|
+----------------+---------+----------+*/

  //Rename DataFrame column

  val df3 = dfDonuts.withColumnRenamed("Donut Name", "Name")
  df3.show()

  /*
+---+-------------+-----+
| Id|         Name|Price|
+---+-------------+-----+
|111|  plain donut|  1.5|
|222|vanilla donut|  2.0|
|333| glazed donut|  2.5|
+---+-------------+-----+
*/

  //Create DataFrame constant column

  val donuts_1 = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df_donuts = sparkSession.createDataFrame(donuts_1).toDF("Donut Name", "Price")

   val df_new = df_donuts
    .withColumn("Tasty", lit(true))
    .withColumn("Correlation", lit(1))
    .withColumn("Stock Min Max", typedLit(Seq(100, 500)))
  df_new.show()

  /*
  * +-------------+-----+-----+-----------+-------------+
|   Donut Name|Price|Tasty|Correlation|Stock Min Max|
+-------------+-----+-----+-----------+-------------+
|  plain donut|  1.5| true|          1|   [100, 500]|
|vanilla donut|  2.0| true|          1|   [100, 500]|
| glazed donut|  2.5| true|          1|   [100, 500]|
+-------------+-----+-----+-----------+-------------+*/

  val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
    case "plain donut"    => Seq(100, 500)
    case "vanilla donut"  => Seq(200, 400)
    case "glazed donut"   => Seq(300, 600)
    case _                => Seq(150, 150)
  }

  val udfStockMinMax = udf(stockMinMax)
  val new_df = df_donuts.withColumn("Stock Min Max", udfStockMinMax($"Donut Name"))
  new_df.show()
  /*
  * +-------------+-----+-------------+
|   Donut Name|Price|Stock Min Max|
+-------------+-----+-------------+
|  plain donut|  1.5|   [100, 500]|
|vanilla donut|  2.0|   [200, 400]|
| glazed donut|  2.5|   [300, 600]|
+-------------+-----+-------------+*/

  //DataFrame First Row
  val firstRow = df_donuts.first()
  println(s"First row = $firstRow")
  /*First row = [plain donut,1.5]*/

  val firstRowColumn1 = df_donuts.first().get(0)
  println(s"First row column 1 = $firstRowColumn1")
  /*First row column 1 = plain donut*/

  val firstRowColumnPrice = df_donuts.first().getAs[Double]("Price")
  println(s"First row column Price = $firstRowColumnPrice")
  /*First row column Price = 1.5*/

  //Format DataFrame column

  val donuts2 = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df5 = sparkSession.createDataFrame(donuts2).toDF("Donut Name", "Price", "Purchase Date")


  df5
    .withColumn("Price Formatted", format_number($"Price", 2))
    .withColumn("Name Formatted", format_string("awesome %s", $"Donut Name"))
    .withColumn("Name Uppercase", upper($"Donut Name"))
    .withColumn("Name Lowercase", lower($"Donut Name"))
    .withColumn("Date Formatted", date_format($"Purchase Date", "yyyyMMdd"))
    .withColumn("Day", dayofmonth($"Purchase Date"))
    .withColumn("Month", month($"Purchase Date"))
    .withColumn("Year", year($"Purchase Date"))
    .show()

  /*
+-------------+-----+-------------+---------------+--------------------+--------------+--------------+--------------+---+-----+----+
|   Donut Name|Price|Purchase Date|Price Formatted|      Name Formatted|Name Uppercase|Name Lowercase|Date Formatted|Day|Month|Year|
+-------------+-----+-------------+---------------+--------------------+--------------+--------------+--------------+---+-----+----+
|  plain donut|  1.5|   2018-04-17|           1.50| awesome plain donut|   PLAIN DONUT|   plain donut|      20180417| 17|    4|2018|
|vanilla donut|  2.0|   2018-04-01|           2.00|awesome vanilla d...| VANILLA DONUT| vanilla donut|      20180401|  1|    4|2018|
| glazed donut|  2.5|   2018-04-02|           2.50|awesome glazed donut|  GLAZED DONUT|  glazed donut|      20180402|  2|    4|2018|
+-------------+-----+-------------+---------------+--------------------+--------------+--------------+--------------+---+-----+----+
*/

  //DataFrame column hashing

  df5
    .withColumn("Hash", hash($"Donut Name")) // murmur3 hash as default.
    .withColumn("MD5", md5($"Donut Name"))
    .withColumn("SHA1", sha1($"Donut Name"))
    .withColumn("SHA2", sha2($"Donut Name", 256)) // 256 is the number of bits
    .show()

  /*
  * +-------------+-----+-------------+----------+--------------------+--------------------+--------------------+
|   Donut Name|Price|Purchase Date|      Hash|                 MD5|                SHA1|                SHA2|
+-------------+-----+-------------+----------+--------------------+--------------------+--------------------+
|  plain donut|  1.5|   2018-04-17|1594998220|53a70d9f08d8bb249...|7882fd7481cb43452...|4aace471ed4433f1b...|
|vanilla donut|  2.0|   2018-04-01| 673697474|254c8f04be947ec2c...|5dbbc954723a74fe0...|ccda17c5bc47d1671...|
| glazed donut|  2.5|   2018-04-02| 715175419|44199f422534a5736...|aaee30ecdc523fa1e...|6d1568ca8c20ffc0b...|
+-------------+-----+-------------+----------+--------------------+--------------------+--------------------+*/


  //DataFrame String Functions

  df5
    .withColumn("Contains plain", instr($"Donut Name", "donut"))
    .withColumn("Length", length($"Donut Name"))
    .withColumn("Trim", trim($"Donut Name"))
    .withColumn("LTrim", ltrim($"Donut Name"))
    .withColumn("RTrim", rtrim($"Donut Name"))
    .withColumn("Reverse", reverse($"Donut Name"))
    .withColumn("Substring", substring($"Donut Name", 0, 5))
    .withColumn("IsNull", isnull($"Donut Name"))
    .withColumn("Concat", concat_ws(" - ", $"Donut Name", $"Price"))
    .withColumn("InitCap", initcap($"Donut Name"))
    .show()

  /*
 * +-------------+-----+-------------+--------------+------+-------------+-------------+-------------+-------------+---------+------+-------------------+-------------+
|   Donut Name|Price|Purchase Date|Contains plain|Length|         Trim|        LTrim|        RTrim|      Reverse|Substring|IsNull|             Concat|      InitCap|
+-------------+-----+-------------+--------------+------+-------------+-------------+-------------+-------------+---------+------+-------------------+-------------+
|  plain donut|  1.5|   2018-04-17|             7|    11|  plain donut|  plain donut|  plain donut|  tunod nialp|    plain| false|  plain donut - 1.5|  Plain Donut|
|vanilla donut|  2.0|   2018-04-01|             9|    13|vanilla donut|vanilla donut|vanilla donut|tunod allinav|    vanil| false|vanilla donut - 2.0|Vanilla Donut|
| glazed donut|  2.5|   2018-04-02|             8|    12| glazed donut| glazed donut| glazed donut| tunod dezalg|    glaze| false| glazed donut - 2.5| Glazed Donut|
+-------------+-----+-------------+--------------+------+-------------+-------------+-------------+-------------+---------+------+-------------------+-------------+
*/

  //DataFrame drop null
  val donuts_null = Seq(("plain donut", 1.50), (null.asInstanceOf[String], 2.0), ("glazed donut", 2.50))
  val dfWithNull = sparkSession
    .createDataFrame(donuts_null)
    .toDF("Donut Name", "Price")

  dfWithNull.show()

  /*
  +------------+-----+
|  Donut Name|Price|
+------------+-----+
| plain donut|  1.5|
|        null|  2.0|
|glazed donut|  2.5|
+------------+-----+ */

  val dfWithoutNull = dfWithNull.na.drop()
  dfWithoutNull.show()

  /*
  +------------+-----+
|  Donut Name|Price|
+------------+-----+
| plain donut|  1.5|
|glazed donut|  2.5|
+------------+-----+*/


}
