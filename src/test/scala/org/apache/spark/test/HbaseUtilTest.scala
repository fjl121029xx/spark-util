package org.apache.spark.test

import org.apache.spark.hbase.core.SparkHBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD

object HbaseUtilTest {
  val tablename = "test"
  val zk="solr1,solr2,datanode37"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("tets")
    val sc = new SparkContext(conf)

    val hc = new SparkHBaseContext(sc, sc.hadoopConfiguration)
    //val rdd = sc.parallelize(Array("1"))
      testBulkRDD(hc)
      .foreach { println}
  }
  def testBulkRDD(hc: SparkHBaseContext)= {
      hc.bulkAllRDD(zk,tablename, f)
  }
  def testBulkGet(hc: SparkHBaseContext, rdd: RDD[String]) {
    val r1 = hc.bulkGetRDD(zk,tablename, rdd, makeGet, convertResult)
  }
  def testBulkScan(hc: SparkHBaseContext) {
    val r2 = hc.bulkScanRDD(zk,tablename, new Scan(), f)
  }

}