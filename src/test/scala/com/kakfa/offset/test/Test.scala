package com.kakfa.offset.test
import java.util.Date
import java.text.SimpleDateFormat
import com.kafka.offset.record.KafkaOffsetRecord
import org.apache.log4j.PropertyConfigurator
object Test {
val smp = new SimpleDateFormat("yyyyMMdd")
PropertyConfigurator.configure("conf/log4j.properties")
val zk="solr1,solr2,mongodb3"
//val zk="zk1,zk2,zk3"
val broker="kafka1,kafka2,kafka3"
  def main(args: Array[String]): Unit = {
  val groupid="kafkatopicoffset"
  val day="20180115"
  var kafkaParams = Map[String, String](
      "metadata.broker.list" ->broker ,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> groupid)
  //KafkaOffsetRecord.recordKafkaOffset(kafkaParams, groupid, day, zk, Set("test"))
  KafkaOffsetRecord
  .getKafkaOffset(kafkaParams, groupid, day, zk, broker, Set("test"))
  .foreach(println)
  }

}