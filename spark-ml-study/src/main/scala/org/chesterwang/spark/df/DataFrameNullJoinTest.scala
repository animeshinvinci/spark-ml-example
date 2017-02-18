package org.chesterwang.spark.df
import util._
import org.apache.spark.sql.functions._

/**
  * Created by chester on 17-1-16.
  */
object DataFrameNullJoinTest {
  def main(args: Array[String]) {
    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameNullJoinTest")

    val df = sqlContext.createDataFrame(
      Array(null.asInstanceOf[String],"2","3").map(Tuple1.apply)
    ).toDF("id")
    df.printSchema()
    df.show(10)
    val joindf = df.join(df,df("id") === df("id"),"left")
    joindf.printSchema()
    joindf.show(10)


    val df3 = sqlContext.createDataFrame(
      Array(null.asInstanceOf[String],"2","2").map(Tuple1.apply)
    ).toDF("id")
    df3.distinct().show(10)


    val df4 = sqlContext.createDataFrame(
      Array((null,"asd"),("2",null),("2","asdf"))
    ).toDF("id","v")
//    df4.select(count("id"),count("v"))
      df4.selectExpr("count(*)")
      .show(10)

  }

}
