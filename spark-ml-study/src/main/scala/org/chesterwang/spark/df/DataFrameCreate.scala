package org.chesterwang.spark.df

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chester on 16-11-18.
  */
object DataFrameCreate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BucketizerExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
//    testBucket(sqlContext)
    testCartesian(sqlContext)

  }

  def testCartesian(sqlContext:SQLContext): Unit ={
    val data = Array(-3, -0.5, -0.3, 0.0, 0.2, 3)
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    df.join(df).show(100)
    import sqlContext.implicits._
    df.select($"features",$"*").show(100)

  }

  def testBucket(sqlContext:SQLContext): Unit ={

    // $example on$
    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-3, -0.5, -0.3, 0.0, 0.2, 3)
    val dataFrame = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)
    bucketedData.show()
    // $example off$

  }

}
