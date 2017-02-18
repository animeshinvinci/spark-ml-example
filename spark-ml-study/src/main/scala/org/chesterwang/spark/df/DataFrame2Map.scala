package org.chesterwang.spark.df

import org.chesterwang.spark.df.util.LocalSparkContext
import org.apache.spark.sql.functions._

/**
  * Created by chester on 17-1-16.
  */
object DataFrame2Map {

  def main(args: Array[String]) {
    val h = LocalSparkContext.getLocalSQLContext("DataFrame2Map")
    val df = h.createDataFrame(Array((1,2),(3,4)))
      .toDF("a","b")
    df.rdd.map(_.getValuesMap(Array("a","b")))
      .take(10)
      .foreach(println)

  }

}
