package com.kakfa.offset.test
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.log4j.PropertyConfigurator
import com.kafka.offset.record.KafkaOffsetUtil
import org.slf4j.LoggerFactory
object Test {
  val smp = new SimpleDateFormat("yyyyMMdd")
  PropertyConfigurator.configure("conf/log4j.properties")
  val LOG=LoggerFactory.getLogger("Test")
  val zk="solr1,solr2,datanode37"
  val broker = "kafka1,kafka2,kafka3"
  val topics = Set("test")
  def main(args: Array[String]): Unit = {
    val groupid = "kafkadayoffset"
    val day = "20180115"
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> groupid)
    val kafkaoffsetUtil = KafkaOffsetUtil(kafkaParams, zk)
    kafkaoffsetUtil.recordDayOffsetsToZK(day, topics)

    val res = kafkaoffsetUtil.getDayOffsetsFromZK(topics, day)
    if (res.isLeft) {
      LOG.info(res.left.get)
    } else res.right.get.foreach(println)
      LOG.info(">>>>>>>>>>>>>")
  }

}