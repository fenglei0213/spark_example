package org.sample.ds

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.sample.ds.ds.DsSample
import org.utils.OfflineUtil

object DsTest {

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val ss = OfflineUtil.getSparkSession(DsSample.getClass.getName, "local")
    val articleDF = this.createArticleDF(ss)
    val authorDF = this.createAuthorDF(ss)
    this.processBiz(articleDF, authorDF, ss)

  }

  /**
    * processBiz processBiz
    *
    * @param articleDF
    * @param authorDF
    * @param ss
    */
  def processBiz(articleDF: DataFrame, authorDF: DataFrame, ss: SparkSession): Unit = {
//    val joinRsDF = articleDF.join(authorDF, "app_id")
//    joinRsDF.show()
//          articleDF.join(authorDF,articleDF("app_id_1")===authorDF("app_id_2"), "left_outer")
//          articleDF.join(authorDF,articleDF("app_id_1")===authorDF("app_id_2"), "leftsemi")
//    val columnRowDF = articleDF
//      .explode("duplist", "duplist_") { duplist: String => duplist.split(",") }
//      .select("rid", "app_id", "title", "duplist_")
//    columnRowDF.show()

    authorDF.createOrReplaceTempView("authorTop")
    val authorOrderByDF = ss.sql("SELECT * FROM (" +
      " SELECT app_id,app_name,is_top,score,ROW_NUMBER()" +
      " OVER (PARTITION BY is_top ORDER BY score DESC) AS row" +
      " FROM authorTop) WHERE row<=2")
    authorOrderByDF.show()

  }


  /**
    * createArticleDF createArticleDF
    *
    * @param ss
    * @return
    */
  def createArticleDF(ss: SparkSession): DataFrame = {
    val articleRdd = OfflineUtil.readFile(ss, "data/ds/article_test")
    val articleSchema = "rid app_id title duplist"
    val fields = articleSchema.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = articleRdd
      .map(_.split(";"))
      .map(attributes =>
        Row(attributes(0), attributes(1), attributes(2), attributes(3))
      )
    ss.createDataFrame(rowRDD, schema)
  }

  /**
    * createAuthorDF createAuthorDF
    *
    * @param ss
    * @return
    */
  def createAuthorDF(ss: SparkSession): DataFrame = {
    val articleRdd = OfflineUtil.readFile(ss, "data/ds/author_test")
    val articleSchema = "app_id app_name is_top score"
    val fields = articleSchema.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = articleRdd
      .map(_.split(";"))
      .map(attributes =>
        Row(attributes(0), attributes(1), attributes(2), attributes(3))
      )
    ss.createDataFrame(rowRDD, schema)
  }

}
