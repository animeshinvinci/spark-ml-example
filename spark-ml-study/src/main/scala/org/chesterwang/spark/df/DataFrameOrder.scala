package org.chesterwang.spark.df

import util._
import org.apache.spark.sql.functions._

/**
  * Created by chester on 17-1-16.
  */
object DataFrameOrder {
  def main(args: Array[String]) {

//    val sc = LocalSparkContext.getLocalSparkContext("DataFrameOrder")
    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameOrder")
    val df = sqlContext.createDataFrame(Array(1,2).map(Tuple1.apply)).toDF("h")
    df.orderBy("h").show(10)

    df.groupBy("h")
      .agg(count("*"))
      .show(10)


    val df2 = sqlContext.createDataFrame(Array(1,2).map(Tuple1.apply)).toDF("h")
    val newdf = df.join(df2,usingColumn = "h")
    newdf.show(10)
    newdf.printSchema()
    newdf.drop(df.col("h")).show(10)

    println(df2.rdd.getClass)



  }

}
