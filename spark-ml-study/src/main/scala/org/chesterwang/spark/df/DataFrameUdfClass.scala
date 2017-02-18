package org.chesterwang.spark.df

import org.chesterwang.spark.df.util.LocalSparkContext
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * Created by chester on 17-1-16.
  */
object DataFrameUdfClass {

  def main(args: Array[String]) {
    val h = LocalSparkContext.getLocalSQLContext("DataFrameUdfClass")
    import h.implicits._

    val df = h.createDataFrame(Array((1,2),(3,4)))
    df.select(
      udf[Personas,Int,Int]((a,b) => new Personas().addPersona(a,b))
        .apply($"a")
    ).show(10)
  }

  class Personas(){
    private val a = mutable.Map[Int,Int]()

    def addPersona(inta:Int,intb:Int): Personas ={
      a.put(inta,intb)
      this
    }
    def getPersona(): Unit ={
      a
    }
  }
}
