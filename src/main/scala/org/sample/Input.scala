package org.sample

import java.net.URLDecoder

import com.alibaba.fastjson.{JSON, JSONPath}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Input {

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
    val lineRDD = sc.textFile("/Users/baidu/code/data/spark_data/input_out")
    val rowRDD = lineRDD.map { x => {
      val split = x.split("\t")
      RowFactory.create(split(0), split(1), split(2),split(3),split(4))
    }
    }
    // b.article_id,b.md5,b.phash,a.rid,a.feed_content
    val schema = StructType(List(
      StructField("article_id", StringType, true),
      StructField("md5", StringType, true),
      StructField("phash", StringType, true),
      StructField("rid", StringType, true),
      StructField("feed_content", StringType, true)

    ))
    val sqlContext = new SQLContext(sc)
    val sourceDF = sqlContext.createDataFrame(rowRDD, schema)

    val mergeRdd = sourceDF.rdd.map(line => {
      val feedContent = line(4).toString
      val jsonObject = JSON.parseObject(feedContent)

      val src = JSONPath.eval(jsonObject, "$.items[?(@.type='image')].data.original.src")
        .toString.replace("\"", "").replace("[", "")
        .replace("]", "")

      def srcDecode: String = URLDecoder.decode(src, "UTF8")

      RowFactory.create(line(0).toString, line(1).toString, line(2).toString, line(3).toString, srcDecode)
      //      RowFactory.create(line(0).toString, line(1).toString, line(2).toString, line(3).toString, feedContent)
    })

    mergeRdd.foreach{println}

  }
}
