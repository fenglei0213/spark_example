package org.wind3stone.spark.sample


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import com.alibaba.fastjson.{JSON, JSONPath}
import java.net.URLDecoder

object Test {

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

    //    mergeDF.show(false)
    val mergeRdd = mergeDF.rdd.map(line => {
      val feedContent = line(3).toString
      val jsonObject = JSON.parseObject(feedContent)

      val src = JSONPath.eval(jsonObject, "$.items[?(@.type='image')].data.original.src")
        .toString.replace("\"", "").replace("[", "")
        .replace("]", "")

      // val sourceMd5Length = line(1).toString().split("/").length
      // val sourceMd5 = line(1).toString.split("/")(sourceMd5Length - 1).split("\\.")(0)
      def srcDecode: String = URLDecoder.decode(src, "UTF8")
      val targetMd5 = srcDecode.substring(srcDecode.indexOf("src=") + 4, srcDecode.indexOf("@"))
      RowFactory.create(line(0).toString, line(1).toString, line(2).toString, targetMd5, src)
    })

    //    rdd.collect().foreach {
    //      println
    //    }

    val molaExSchema = StructType(List(
      StructField("article_id", StringType, true),
      StructField("pngMd5", StringType, true),
      StructField("phash", StringType, true),
      StructField("targetPngMd5", StringType, true),
      StructField("timgArray", StringType, true)
    ))
    val molaExDF = sqlContext.createDataFrame(mergeRdd, molaExSchema)
    //    molaExDF.rdd.saveAsTextFile("/Users/baidu/code/data/spark_data/map_out")
    val molaExExplode = molaExDF.explode("timgArray", "timgArray_") { timg: String => timg.split(",") }
    //molaExExplode.rdd.saveAsTextFile("/Users/baidu/code/data/spark_data/map_out")
    
    val molaDistinct = molaExExplode.where("pngMd5=targetPngMd5").dropDuplicates(Array("pngMd5"))

    molaDistinct.show(false)
  }
}