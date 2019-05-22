package com.aravinth.spark.common

import com.aravinth.spark.utils.WithSpark
import org.apache.spark.sql.Dataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object spark_operation extends App with WithSpark{

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val sc = sparkSession.sparkContext

  /**********Map***************/

  // Basic map example in scala
  val x = sc.parallelize(List("spark", "rdd", "example",  "sample", "example"), 3)
  val y = x.map(x => (x, 1))
  println(y.collect.toList)
  // res0: Array[(String, Int)] =
  //    Array((spark,1), (rdd,1), (example,1), (sample,1), (example,1))

  // rdd y can be re writen with shorter syntax in scala as
  val y1 = x.map((_, 1))
  println(y1.collect.toList)
  // res1: Array[(String, Int)] =
  //    Array((spark,1), (rdd,1), (example,1), (sample,1), (example,1))

  // Another example of making tuple with string and it's length
  val y2 = x.map(x => (x, x.length))
  println(y2.collect.toList)
  // res3: Array[(String, Int)] =
  // Array((spark,5), (rdd,3), (example,7), (sample,6), (example,7))

  /***********Flatmap***************/

  val x1 = sc.parallelize(List("spark rdd example",  "sample example"), 2)

  // map operation will return Array of Arrays in following case : check type of res0
  val y3 = x1.map(x => x.split(" ")) // split(" ") returns an array of words
  y3.collect
  // res0: Array[Array[String]] =
  //  Array(Array(spark, rdd, example), Array(sample, example))

  // flatMap operation will return Array of words in following case : Check type of res1
  val y4 = x1.flatMap(x => x.split(" "))
  y4.collect
  //res1: Array[String] =
  //  Array(spark, rdd, example, sample, example)

  // RDD y can be re written with shorter syntax in scala as
  val y5 = x1.flatMap(_.split(" "))
  y5.collect
  //res2: Array[String] =
  //  Array(spark, rdd, example, sample, example)

  /************Filter*****************/

  val x3 = sc.parallelize(1 to 10, 2)

  // filter operation
  val y6 = x3.filter(e => e%2==0)
  y6.collect
  // res0: Array[Int] = Array(2, 4, 6, 8, 10)

  // RDD y can be re written with shorter syntax in scala as
  val y7 = x3.filter(_ % 2 == 0)
  y7.collect

  /************Reduce*****************/

  // reduce numbers 1 to 10 by adding them up
  val x4 = sc.parallelize(1 to 10, 2)
  val y8 = x4.reduce((accum,n) => (accum + n))
  // y: Int = 55

  // shorter syntax
  val y9 = x4.reduce(_ + _)
  // y: Int = 55

  // same thing for multiplication
  val y10 = x4.reduce(_ * _)
  // y: Int = 3628800

  /************ReduceByKey*****************/

  // Bazic reduceByKey example in scala
  // Creating PairRDD x with key value pairs
  val x5 = sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1),
    ("a", 1), ("b", 1), ("b", 1),("b", 1), ("b", 1)), 3)
  // x: org.apache.spark.rdd.RDD[(String, Int)] =
  //  ParallelCollectionRDD[1] at parallelize at <console>:21

  // Applying reduceByKey operation on x
  val y11 = x5.reduceByKey((accum, n) => (accum + n))
  // y: org.apache.spark.rdd.RDD[(String, Int)] =
  //  ShuffledRDD[2] at reduceByKey at <console>:23

  y11.collect
  // res0: Array[(String, Int)] = Array((a,3), (b,5))

  // Another way of applying associative function
  val y12 = x5.reduceByKey(_ + _)
  // y: org.apache.spark.rdd.RDD[(String, Int)] =
  //  ShuffledRDD[3] at reduceByKey at <console>:23

  y12.collect
  // res1: Array[(String, Int)] = Array((a,3), (b,5))

  // Define associative function separately
  def sumFunc(accum:Int, n:Int) =  accum + n

  val y13 = x5.reduceByKey(sumFunc)
  // y: org.apache.spark.rdd.RDD[(String, Int)] =
  //  ShuffledRDD[4] at reduceByKey at <console>:25

  y13.collect
  // res2: Array[(String, Int)] = Array((a,3), (b,5))

  /***************GroupBy******************/
  // Bazic groupBy example in scala
  val x6 = sc.parallelize(Array("Joseph", "Jimmy", "Tina", "Thomas", "James", "Cory", "Christine", "Jackeline", "Juan"), 3)
  // x: org.apache.spark.rdd.RDD[String] =
  //  ParallelCollectionRDD[16] at parallelize at <console>:21

  // create group per first character
  val y14 = x6.groupBy(word => word.charAt(0))
  // y: org.apache.spark.rdd.RDD[(Char, Iterable[String])] =
  //  ShuffledRDD[18] at groupBy at <console>:23

  y14.collect
  // res0: Array[(Char, Iterable[String])] =
  //  Array((T,CompactBuffer(Tina, Thomas)), (C,CompactBuffer(Cory, Christine)), (J,CompactBuffer(Joseph, Jimmy, James, Jackeline, Juan)))

  // Another short syntax
  val y15 = x6.groupBy(_.charAt(0))
  // y: org.apache.spark.rdd.RDD[(Char, Iterable[String])] =
  //  ShuffledRDD[3] at groupBy at <console>:23

  y15.collect
  // res1: Array[(Char, Iterable[String])] =
  //  Array((T,CompactBuffer(Tina, Thomas)),
  //        (C,CompactBuffer(Cory, Christine)),
  //        (J,CompactBuffer(Joseph, Jimmy, James, Jackeline, Juan)))

  /***************GroupByKey******************/



}
