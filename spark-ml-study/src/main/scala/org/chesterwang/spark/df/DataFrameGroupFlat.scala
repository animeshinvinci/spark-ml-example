package org.chesterwang.spark.df


import org.apache.spark.sql.Row
import util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * Created by chester on 17-2-7.
  */
object DataFrameGroupFlat {

  def main(args: Array[String]) {


    //    val sc = LocalSparkContext.getLocalSparkContext("DataFrameOrder")
    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameOrder")
    import sqlContext.implicits._
    val df = sqlContext.createDataFrame(
      Array(
        (1,2,3),
        (3,2,3),
        (1,9,3)
      )
    ).toDF("h","x","y")

    val a = df.groupBy("h")


  }

}
