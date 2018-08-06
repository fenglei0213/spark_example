package org.wind3stone.spark.sample


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataTypes

object TestCanKao {

  def parseTimg(str: String): String = {

    str
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //    conf.setMaster("local")
    conf.setMaster("local[*]")
    conf.setAppName("Test App")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)
    //

    //    val sparkSession = SparkSession.builder
    //      .getOrCreate()

    // val lineRDD = sc.textFile("afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily/20180621_test/*")
    val lineRDD = sc.textFile("/Users/baidu/code/data/spark_data/in_1")
    val rowRDD = lineRDD.map { x => {
      val split = x.split("\t")
      RowFactory.create(split(3), split(10), split(11))
    }
    }
    val schema = StructType(List(
      StructField("article_id", StringType, true),
      StructField("md5", StringType, true),
      StructField("phash", StringType, true)
    ))
    val sqlContext = new SQLContext(sc)
    val sourceDF = sqlContext.createDataFrame(rowRDD, schema)
    //.createOrReplaceTempView("sourceDF")

    /*
    val molaDF = sqlContext.sql(
          s"""
             |SELECT b.article_id,md5,phash,a.feed_content FROM hpb_ods_bjh_article_mola_df a JOIN sourceDF b
             | ON a.article_c_id=b.article_id AND a.event_day=20180718 LIMIT 1
             """.stripMargin)
             */


    // val lineMolraRDD = sc.textFile("afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily/mola_test/*")
    val molaLine = sc.textFile("/Users/baidu/code/data/spark_data/b_out_out")
    val molaRowRDD = molaLine.map { x => {
      val split = x.split("\t")
      RowFactory.create(split(0), split(1))
    }
    }
    val molaSchema = StructType(List(
      StructField("article_id", StringType, true),
      StructField("feed_content", StringType, true)
    ))
    val molaDF = sqlContext.createDataFrame(molaRowRDD, molaSchema)
    //.createOrReplaceTempView("sourceDF")
    val mergeDF = sourceDF.join(molaDF, "article_id")
    val mergeV2DF = mergeDF.map(
      new MapFunction[Row, String] {
        override def call(value: Row): String = {
          value.getAs[String]("feed_content").toString.substring(0, 1)
        }
      }, Encoders.STRING
    )
    mergeV2DF.show()
  }
}