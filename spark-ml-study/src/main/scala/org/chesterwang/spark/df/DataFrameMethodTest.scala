package org.chesterwang.spark.df

import org.apache.spark.sql.{Row, SQLContext}
import org.chesterwang.spark.df.util.LocalSparkContext
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * Created by chester on 17-2-18.
  */
object DataFrameMethodTest {

  implicit val sqlContext = LocalSparkContext.getLocalSQLContext("DataFrame2Map")

  import sqlContext.implicits._

  def main(args: Array[String]) {
    //    row2Map()
    //    createDf()
    //    orderDf()
    //    groupCount()
    //    mapTypeColumn()
    //    nullValueJoin
    //    userDefinedType
    //    withColumn()
    //    selectColumn
    //    emptyDataFrame
    arrayUdf()


  }

  /**
    * 含有array类型的udf
    */
  def arrayUdf(): Unit = {
    sqlContext.udf.register("flat_concat_json",
      (x: mutable.WrappedArray[String]) => x.mkString(",")
    )
    val data = Array(Some(-3), None)
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    df.selectExpr("""flat_concat_json(array("a","b"))""").show(10)

  }

  def orderDf(): Unit = {

    sqlContext.createDataFrame(
      Array(2, 1).map(Tuple1.apply)
    ).toDF("h")
      .orderBy("h")
      .show(10)

  }

  def row2Map(): Unit = {
    val df = sqlContext.createDataFrame(
      Array(
        (1, 2),
        (3, 4)
      )
    ).toDF("a", "b").withColumn("merge", struct("a", "b"))
    df.rdd.map(_.getValuesMap(Array("a", "b")))
      .take(10)
      .foreach(println)
    df.rdd.map(row => row.getValuesMap(row.schema.fieldNames))
      .take(10)
      .foreach(println)
    df.rdd.map(row => row.getStruct(2))
      .map(row => row.getValuesMap(Array("a", "b")))
      .take(10)
      .foreach(println)
    df.rdd.map(row => row.getStruct(2))
      .map(row => row.schema.fieldNames.mkString(","))
      .take(10)
      .foreach(println)
  }

  def createDf(): Unit = {

    //cartesian
    val data = Array(-3, -0.5, -0.3)
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    df.join(df).show(100)
    df.select($"features", $"*").show(100)

  }

  def groupCount(): Unit = {

    sqlContext.createDataFrame(
      Array(2, 1).map(Tuple1.apply)
    ).toDF("h")
      .groupBy("h")
      .agg(count("*"))
      .show(10)
  }

  //todo
  def groupFlat(): Unit = {

    val df = sqlContext.createDataFrame(
      Array(
        (1, 2, 3),
        (3, 2, 3),
        (1, 9, 3)
      )
    ).toDF("h", "x", "y")
    val a = df.groupBy("h")
  }

  def mapTypeColumn(): Unit = {
    val df = sqlContext.createDataFrame(
      Array(Tuple1(1), Tuple1(3), Tuple1(1))
    ).toDF("h")

    val df2 = df.withColumn("mm",
      udf[Map[String, Int], String] {
        (x: String) =>
          Map(x -> 1)
      }.apply($"h")
    )
    df2.show(10)
    df2.printSchema()
  }

  /**
    * null value join的时候 null === null 是为true的,可以join成功
    */
  def nullValueJoin(): Unit = {

    val df = sqlContext.createDataFrame(
      Array(null.asInstanceOf[String], "2", "3").map(Tuple1.apply)
    ).toDF("id")
    df.printSchema()
    df.show(10)
    val joindf = df.join(df, df("id") === df("id"), "left")
    joindf.printSchema()
    joindf.show(10)
  }

  def userDefinedType(): Unit = {

    val df = sqlContext.createDataFrame(Array((1, 2), (3, 4)))
    try {

      df.select(
        udf[Personas, Int, Int]((a, b) => new Personas().addPersona(a, b))
          .apply($"a")
      ).show(10)

    } catch {
      case e: Exception => println("不支持Persona这种类型")
    }

  }

  /**
    * withColumn添加列的几种形式
    */
  def withColumn(): Unit = {

    val df = sqlContext.createDataFrame(
      Array(null.asInstanceOf[String], "2", "3").map(Tuple1.apply)
    ).toDF("id")
    df.show(10)

    val df2 = sqlContext.createDataFrame(
      Array("h", "p").map(Tuple1.apply)
    ).toDF("a1")

    //1. 内置函数形式
    df.withColumn("co", lit(true)).show(10)
    //2. udf形式
    df.withColumn("co", udf[Int, Int] { x: Int => x + 1 }.apply(col("id"))).show(10)
    //3. 原始列直接复制
    df.withColumn("co", df.col("id")).show(10)
    //4. 原始列直接复制
    df.withColumn("co", col("id")).show(10)
  }

  /**
    * 判断一行中是否有空值
    */
  def anyNullColumn(): Unit = {

    val df = sqlContext.createDataFrame(
      Array(null.asInstanceOf[String], "2", "3").map(Tuple1.apply)
    ).toDF("id")
    df.show(10)

    df.withColumn("merge_cnt",
      udf[Boolean, Row] {
        x: Row => x.anyNull
      }.apply(col("merge"))
    ).show(10)

  }

  /**
    * select的几种形式
    */
  def selectColumn(): Unit = {

    val df = sqlContext.createDataFrame(
      Array(null.asInstanceOf[String], "2", "3").map(Tuple1.apply)
    ).toDF("id")
    df.show(10)

    //1. 所有列
    df.selectExpr("*").show(10)
    //2. 列名 和函数混合
    df.selectExpr("id", "isnull(id)").show(10)
    //3. select方法只能选择列名
    df.select("id").show(10)
    //4. 结构体
    df.select(struct("id").as("hh"))
      .select("hh.id").show(10)

  }

  def emptyDataFrame(): Unit = {
    val df = sqlContext.emptyDataFrame
    df.printSchema()
  }

  class Personas() {
    private val a = mutable.Map[Int, Int]()

    def addPersona(inta: Int, intb: Int): Personas = {
      a.put(inta, intb)
      this
    }
  }


}
