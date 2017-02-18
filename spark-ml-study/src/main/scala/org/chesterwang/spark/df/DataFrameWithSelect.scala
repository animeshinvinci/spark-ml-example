package org.chesterwang.spark.df

import org.apache.spark.sql.Row
import org.chesterwang.spark.df.util._
import org.apache.spark.sql.functions._

/**
  * Created by chester on 17-1-16.
  */
object DataFrameWithSelect {
  def main(args: Array[String]) {
    val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrameNullJoinTest")
    import sqlContext.implicits._

    val df = sqlContext.createDataFrame(
      Array(null.asInstanceOf[String],"2","3").map(Tuple1.apply)
    ).toDF("id")

    df.show(10)

//    df.selectExpr("*").show(10)

//    df.select("id","isnull(id,1,0)").show(10)

//    df.select(struct("id").as("hh"))
//        .select("hh(0)").show(10)

//    df.withColumn("",col("").equalTo(col("")).or())
    val aa =  df.withColumn("co",lit(true))
      aa.show(10)
    aa.printSchema()

    val df2 = df.withColumn("merge",struct("id"))

    df2.withColumn("merge_cnt",udf[Boolean, Row]{x:Row => x.anyNull}.apply(col("merge")))
      .show(10)






  }

}
