package org.chesterwang.spark.df

import org.chesterwang.spark.df.util.LocalSparkContext

/**
  * Created by chester on 17-1-16.
  */
object EmptyDataFrameTest {
  def main(args: Array[String]) {

    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameOrder")
    val df = sqlContext.emptyDataFrame
    df.printSchema()
    df.toDF("h").printSchema()


  }

}
