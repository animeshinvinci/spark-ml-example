package org.chesterwang.spark.df


import org.apache.spark.sql.Row
import util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * Created by chester on 17-2-8.
  */
object DataFrameMapSupport {
  def main(args: Array[String]) {

    //    val sc = LocalSparkContext.getLocalSparkContext("DataFrameOrder")
    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameOrder")
    import sqlContext.implicits._
    val df = sqlContext.createDataFrame(
      Array(
        (1, 2, 3),
        (3, 2, 3),
        (1, 9, 3)
      )
    ).toDF("h", "x", "y")


    val df2 = df.withColumn("mm",
      udf[Map[String, Int], String] {
        (x: String) =>
          Map(x -> 1)
      }.apply($"h")
    )
    df2.show(10)

    val df3 = df2.withColumn("hhhh",udf[String,Map[String,Int]]{
      x:Map[String,Int] => x.toString
    }.apply($"mm"))
    df3.show(10)
    df3.printSchema()

//    val a = DataFrameMapSupport.f (_)
//
//
//    sqlContext.udf.register("hahaha",a)
//
//    df3.withColumn("merge",
//      callUDF("hahaha",)
//    )

  }
}
