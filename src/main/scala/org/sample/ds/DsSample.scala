package org.sample.ds.ds

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.utils.OfflineUtil

object DsSample {


  def main(args: Array[String]): Unit = {
    val ss = OfflineUtil.getSparkSession(DsSample.getClass.getName, "local")
    //    val articleDF = this.createArticleDF4UDW(ss);
    val articleDF = this.createArticleDF4file(ss)
    // 处理数据
    this.bizProcess(ss, articleDF)
    // 输出
    this.dataOutPut()
    ss.stop()
  }

  def dataOutPut(): Unit ={

  }

  /**
    * bizProcess bizProcess
    *
    * @param ss
    * @param articleDF
    */
  def bizProcess(ss: SparkSession, articleDF: DataFrame): Unit = {
    articleDF.createOrReplaceTempView("bjh_dim_essay_df")
        val articleRs = ss.sql("SELECT get_json_object(ext_info,'$.feed.majia_from') " +
          " majia_from FROM bjh_dim_essay_df WHERE" +
          " get_json_object(ext_info,'$.feed.majia_from') is not null LIMIT 5")
        articleRs.show()

//
//        val articleRs = articleDF.select("rid")
//          .where("is_own=1")
//        articleRs.show(5)
    

  }

  /**
    * createArticleDF createArticleDF
    *
    * @param ss
    */
  def createArticleDF4file(ss: SparkSession): DataFrame = {
    val articleRdd = OfflineUtil.readFile(ss, "data/ds/article_sample")
    val articleSchema = "rid publish_at source_name is_own ext_info"
    val fields = articleSchema.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = articleRdd
      .map(_.split("\t"))
      .map(attributes =>
        Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4))
      )
    ss.createDataFrame(rowRDD, schema)
  }

  /**
    * createArticleDF createArticleDF
    *
    * @param ss
    */
  def createAuthorDF4file(ss: SparkSession): DataFrame = {
    val articleRdd = OfflineUtil.readFile(ss, "data/ds/author_sample")
    val articleSchema = "rid publish_at source_name is_own ext_info"
    val fields = articleSchema.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = articleRdd
      .map(_.split("\t"))
      .map(attributes =>
        Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4))
      )
    ss.createDataFrame(rowRDD, schema)
  }

  /**
    * createArticleDF4UDW createArticleDF4UDW
    *
    * @param ss
    */
  def createArticleDF4UDW(ss: SparkSession): DataFrame = {
    val articleDF = ss.sql(
      s"""
         |SELECT a.rid,a.publish_at,a.source_name,a.is_own,a.ext_info" +
         | FROM bjh_dim_essay_df a WHERE a.event_day=20180629 LIMIT 1000" +
          """.stripMargin);
    articleDF
  }

}
