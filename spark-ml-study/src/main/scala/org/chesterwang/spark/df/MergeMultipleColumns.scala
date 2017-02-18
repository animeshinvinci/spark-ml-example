package org.chesterwang.spark.df

import org.apache.spark.sql.Row
import util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * Created by chester on 17-2-7.
  */
object MergeMultipleColumns {
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

    val df2 = df.select(struct("h","x","y"))
    df2.printSchema()
      df2.show(10)
    val df3 = df.withColumn("merge",struct("h","x","y"))
    df3.printSchema()
      df3.show(10)

//    val df4 =df3.select("merge").map(x => x.getAs[Row]("merge"))

    df3.withColumn("mergestring",
      udf[String,Row]{
        r:Row =>
          {
//            r.getValuesMap[String](Seq[String]("h","x","y")).toString()
            Array("h","x","y").zip(r.toSeq).mkString(",")
//            r.schema.toString()
          }
      }.apply($"merge")
    ).show(10,truncate = false)

    df.show(10)

  }

}
