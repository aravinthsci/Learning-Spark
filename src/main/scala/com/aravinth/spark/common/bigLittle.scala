package com.aravinth.spark.common

import com.aravinth.spark.utils.WithSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object bigLittle extends App with WithSpark{

  import sparkSession.implicits._

  val product_df = sparkSession.read.option("header",true).csv("src/main/resources/Product.csv")
  var sales_df = sparkSession.read.option("header",true).option("inferSchema", "true").csv("src/main/resources/Sale.csv")

  //sales_df = sales_df.withColumn("day", sales_df("created_at").cast(DateType))


  product_df.createOrReplaceTempView("product")
  sales_df.createOrReplaceTempView("sales")

  val df = sparkSession.sql("select date_format(created_at,\"yyyyMMdd\") as day,s.product_id,p.name,s.units , (s.units * p.unit_price) price from sales s JOIN product p on s.product_id = p.id  ")

  df.createOrReplaceTempView("temp")
  val new_df = sparkSession.sql("select day,sum(price) price,name from temp group by day,name order by day asc,name asc,price desc")
  new_df.createOrReplaceTempView("temp1")
  val final_df = sparkSession.sql("select day,price,collect_list(name) top from temp1 group by day,price order by day asc,price desc")


  val w = Window.partitionBy("day").orderBy($"day".asc,$"price".desc)

  val result_df = final_df.select($"*",row_number().over(w).alias("row_number")).where("row_number == 1").drop("row_number","price").orderBy($"day".asc)

  val topAsSingleString = result_df.as[(String, Array[String])].map { case (day, top) => (day, top.mkString(",")) }.toDF("day", "top")


  topAsSingleString.show(100)
  topAsSingleString.repartition(1).write.option("header",true).csv("result")


}
