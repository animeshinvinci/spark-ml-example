package org.chesterwang.spark.df.parseJson

import org.apache.spark.sql.Row

import util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.chesterwang.spark.df.util.LocalSparkContext

/**
  * Created by chester on 17-2-9.
  */
object DataFrameRddSubField {
  def main(args: Array[String]) {

    //    val sc = LocalSparkContext.getLocalSparkContext("DataFrameOrder")
    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameOrder")
    import sqlContext.implicits._
    val df = sqlContext.createDataFrame(
      Array(
        (2,Example(1,2)),
        (3,Example(3,2))
      )
    ).toDF("h","x")
    df.show(10)
//    df.rdd.map(x => x.getAs[Int]("x.a"))
//      .take(10).foreach(println)

    df.select(col("h").as("h"),col("x.a").as("x.a")).rdd
      .map(x => x.schema.fieldNames)
      .take(10).foreach(x => println(x.mkString(",")))

    df.select(col("h").as("h"),col("x.a").as("x.a")).rdd
      .map(x => x.getAs[Int]("x.a"))
      .take(10).foreach(x => println(x))
  }

  case class Example(a:Int,b:Int)

}
