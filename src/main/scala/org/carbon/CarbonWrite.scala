package org.carbon

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession

object CarbonWrite {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val baseHome = "file:///code/icode/bjh-tomato/data"

    val storeLocation = s"$baseHome/store"
    val warehouse = s"$baseHome/warehouse"
    val metastoredb = s"$baseHome/metastoredb"

    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)
//    val inputPath = "afs://szth.afs.baidu.com:9902/user/feed-bjh/users/fenglei/carbon/test/insert_test"
    val inputPath = s"$baseHome/input/essay"
    sqlContext.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputPath'
         | INTO TABLE carbon_bjh_dim_essay_df
         | options('FILEHEADER'='rid,title,url,status')
       """.stripMargin).createTempView("carbon_bjh_dim_essay_df")

    sqlContext.sql("select COUNT(1) from carbon_bjh_dim_essay_df").show()
  }

}
