
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import com.alibaba.fastjson.{JSON, JSONPath}
import java.net.URLDecoder
import org.apache.spark.sql.Encoders

import org.apache.spark.api.java.function.MapFunction

object PicService {

  def main(args: Array[String]): Unit = {

    //    val startDay = args(0)
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    conf.setAppName("Test App")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    //    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("yarn")
    //    conf.set("spark.executor.cores", "2")
    //    conf.set("spark.executor.memory", "10g")
    //    conf.set("spark.executor.instances", "40")
    val sc = new SparkContext(conf)

    //    val lineRDD = sc.textFile(s"afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily_out/part-03999")
    val lineRDD = sc.textFile(s"/Users/baidu/code/data/spark_data/part-03999")
    // lineRDD.saveAsTextFile(s"afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily_out/test")

    //    val lineRDD = sc.textFile(s"afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily/$startDay/*")
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
    //      .createOrReplaceTempView("sourceDF")

    //    val molaLine = sc.textFile("afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily_out/mola_test/*")
    val molaLine = sc.textFile(s"/Users/baidu/code/data/spark_data/b_out")
    val molaRowRDD = molaLine.map { x => {
      val split = x.split("\t")
      RowFactory.create(split(0), split(1), split(2))
    }
    }
    val molaSchema = StructType(List(
      StructField("rid", StringType, true),
      StructField("article_id", StringType, true),
      StructField("feed_content", StringType, true)
    ))
    val molaDF = sqlContext.createDataFrame(molaRowRDD, molaSchema)
    val mergeDF = sourceDF.join(molaDF, "article_id")

    // mergeDF.rdd.saveAsTextFile(s"afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily_out/20180719")

//    val mergeDF = sqlContext.sql(
    //      s"""
    //         |SELECT b.article_id,md5,phash,a.rid,a.feed_content FROM hpb_ods_bjh_article_mola_df a JOIN sourceDF b
    //         | ON a.article_c_id=b.article_id AND a.event_day=20180719
    //                 """.stripMargin)

    //
    val mergeRdd = mergeDF.rdd.map(line => {
      val feedContent = line(4).toString
      val jsonObject = JSON.parseObject(feedContent)

      val src = JSONPath.eval(jsonObject, "$.items[?(@.type='image')].data.original.src")
        .toString.replace("\"", "").replace("[", "")
        .replace("]", "")

      def srcDecode: String = URLDecoder.decode(src, "UTF8")

      RowFactory.create(line(0).toString, line(1).toString, line(2).toString, line(3).toString, srcDecode)
    })
    // [1606306549806086148,http://pic.rmb.bdstatic.com/eebb9542d986f3f5579742084f0a1c95.jpeg,ab6ca8528ad3ead4,8966747663507367745,,]

    //    mergeRdd.saveAsTextFile(s"afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily_out/$startDay/")

    val molaExSchema = StructType(List(
      StructField("article_id", StringType, true),
      StructField("pngMd5", StringType, true),
      StructField("phash", StringType, true),
      StructField("rid", StringType, true),
      StructField("targetPngMd5Array", StringType, true)
    ))
    val molaExDF = sqlContext.createDataFrame(mergeRdd, molaExSchema)
    val molaExExplode = molaExDF
      .explode("targetPngMd5Array", "targetPngMd5ArrayItem") { timg: String => timg.split(",") }

    val molaExExplodeMapRdd = molaExExplode.rdd.map(line => {
      var targetMd5Url = ""
      val targetPngMd5Item = line(5).toString
      if (targetPngMd5Item.contains(".jpeg")) {
        targetMd5Url = targetPngMd5Item.substring(targetPngMd5Item.indexOf("src=") + 4, targetPngMd5Item.indexOf(".jpeg") + 5)
      } else if (targetPngMd5Item.contains(".png")) {
        targetMd5Url = targetPngMd5Item.substring(targetPngMd5Item.indexOf("src=") + 4, targetPngMd5Item.indexOf(".png") + 4)
      }
      RowFactory.create(line(0).toString,
        line(1).toString, line(2).toString, line(3).toString, line(5).toString, targetMd5Url)
    })

    val finalSchema = StructType(List(
      StructField("article_id", StringType, true),
      StructField("pngMd5", StringType, true),
      StructField("phash", StringType, true),
      StructField("rid", StringType, true),
      StructField("targetPngMd5ArrayItem", StringType, true),
      StructField("targetPngMd5ItemUrl", StringType, true)
    ))
    val finalDF = sqlContext.createDataFrame(molaExExplodeMapRdd, finalSchema)
    val molaDistinct = finalDF.where("pngMd5=targetPngMd5ItemUrl").dropDuplicates(Array("pngMd5"))
      .select("rid", "pngMd5","targetPngMd5ArrayItem", "phash")
    molaDistinct.show(false)
    // molaDistinct.rdd.saveAsTextFile(s"afs://szth.afs.baidu.com:9902/user/feed-bjh/users/tanyang/get_article_pic_phash_daily_out/$startDay/")
    //    molaDistinct.saveAsTextFile(s"/Users/baidu/code/data/spark_data/out")
  }
}