package org.chesterwang.spark.df

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, Vector => MLVector}
import org.chesterwang.spark.df.util.LocalSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

/**
  * Created by chester on 17-2-8.
  */
object DataFrameGetStructSubField {

  def main(args: Array[String]) {


    //    val sc = LocalSparkContext.getLocalSparkContext("DataFrameOrder")
    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameOrder")
    import sqlContext.implicits._

    val df = sqlContext.createDataFrame(
      Array(LabeledPoint(1.0,Vectors.sparse(10,Array(1,3,5),Array(0.1,8,3.2))))
        .map(Tuple1.apply)
    ).toDF("lp")

    df.select("lp.features").show(10)

    df.select("lp").rdd
      .map(x => x.getStruct(0).getValuesMap(Seq("label","features")))
//      .map(x => x.getStruct(0).schema.fieldNames.mkString(","))
      .take(10)
      .foreach(println)

    //合并


//    df.withColumn("sssl",udf[String,Row]{
//      x:Row => {
//        x.schema.fieldNames.mkString(",")
////        x.getValuesMap(Seq("lp")).toString()
//        x.getAs[Int]("lp").toString
//      }
//    }.apply($"lp"))
//      .show(10)


    val sdf = sqlContext.createDataFrame(
      Array(
        (1,2,3),
        (3,2,3),
        (1,9,3)
      )
    ).toDF("h","x","y")


//    val f = ( a: Int,b:Int* ) => a

//    sqlContext.udf.register("simpleUDF", )
    sdf.withColumn("output", callUdf("concat",Array("h","x","y").map(x => col(x)):_*))
      .show(10)

  }

//  def f(a:Int*) = a.toArray.sum


}
