package org.chesterwang.spark.df

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by chester on 16-12-10.
  */
object OptionDataFrame {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BucketizerExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    //    testBucket(sqlContext)
    test1(sqlContext)

  }

  def test1(sqlContext:SQLContext): Unit ={

    sqlContext.udf.register("flat_concat_json",
      (x:mutable.WrappedArray[String]) => x.mkString(",")
    )
    val data = Array(Some(-3), None)
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    import sqlContext.implicits._
    df.select($"features",$"*").show(100)
    df.printSchema()
    df.selectExpr("""flat_concat_json(array("a","b"))""")
      .show(10)



  }

}
