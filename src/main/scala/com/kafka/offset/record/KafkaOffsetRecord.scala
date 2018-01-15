package com.kafka.offset.record

import java.util.Date
import java.text.SimpleDateFormat
import com.kafka.zk.util.ZookeeperUtil
import com.kafka.zk.util.KafkaUtil
import kafka.common.TopicAndPartition
import org.slf4j.LoggerFactory

/**
 * 功能：用于记录kafka的topic的某个时刻的偏移量
 */
object KafkaOffsetRecord {
  val LOG = LoggerFactory.getLogger("KafkaOffsetRecord")
  /**
   * 功能：将kafka的offset写入zookeepr
   * 注： 将topic的偏移量写入指定的path下面，用topicname做文件夹名
   * 如：path=./groupid/day/
   * 那 topic=test的就会./groupid/day/test下
   */
  def recordKafkaOffset(
    kafkaParams: Map[String, String],
    path: String,
    zk: String,
    topics: Set[String]) {
    val zkClient = ZookeeperUtil.getzkClient(zk)
    ZookeeperUtil.createMultistagePath(zkClient, path)
    val util = new KafkaUtil(kafkaParams)
    val topicpart = if (null == topics || topics.isEmpty) {
      val alltopics = util.getAlltopics(zkClient).toSet
      LOG.warn("Use Defualt Setting - AllTopic : ", alltopics.mkString(","))
      util.getLatestLeaderOffsets(alltopics, zkClient)
    } else {
      util.getLatestLeaderOffsets(topics, zkClient)
    }
    topicpart
      .map { case (tp, offset) => (tp.topic, tp.partition, offset) }
      .toList
      .groupBy(_._1)
      .foreach {
        case (topic, list) =>
          val topicPath = s"""${path}/${topic}"""
          ZookeeperUtil.createFileOrDir(zkClient, topicPath, "")
          list.foreach {
            case (topic, part, offset) =>
              val path = s"""${topicPath}/${part}"""
              ZookeeperUtil.writeData(zkClient, path, offset.toString)
          }
      }

  }
  /**
   * 功能：将kafka的offset写入zookeepr
   * 注： 将topic的偏移量写入指定的path下面，用topicname做文件夹名
   * 如：path=./groupid/day/
   * 那 topic=test的就会./groupid/day/test下
   */
  def recordKafkaOffset(
    kafkaParams: Map[String, String],
    groupid: String,
    day: String,
    zk: String,
    topics: Set[String] = null) {
    val path = s"""/consumers/${groupid}/offsets/${day}"""
    LOG.info("PATH : " + path)
    recordKafkaOffset(kafkaParams, path, zk, topics)
  }
  /**
   * 与上面的对应,获取topic的偏移量
   * 可以与spark-kafka来结合使用
   */
  def getKafkaOffset(
    kafkaParams: Map[String, String],
    groupid: String,
    day: String,
    zk: String,
    broker: String,
    topics: Set[String] = null
    ): Map[TopicAndPartition, Long] = {
    val path = s"""/consumers/${groupid}/offsets/${day}"""
    getKafkaOffset(kafkaParams, path, zk, broker, topics)
  }
  /**
   * 与上面的对应,获取topic的偏移量
   * 可以与spark-kafka来结合使用
   */
  def getKafkaOffset(
    kafkaParams: Map[String, String],
    path: String,
    zk: String,
    broker: String,
    topics: Set[String]
    ): Map[TopicAndPartition, Long] = {
    val zkClient = ZookeeperUtil.getzkClient(zk)
    val util = new KafkaUtil(kafkaParams)
    if (ZookeeperUtil.isExist(zkClient, path)) {
      val topicinfo=if (topics == null || topics.isEmpty) {
        val alltopics = util.getAlltopics(zkClient).toSet
        LOG.warn("Use Defualt Setting - AllTopic : ", alltopics.mkString(","))
        util.getTopicAndPartitions(alltopics)
      } else {
        util.getTopicAndPartitions(topics)
      }
      val map=scala.collection.mutable.Map[TopicAndPartition, Long]()
      topicinfo
      .map { x => (x.topic,x.partition) }
      .groupBy(_._1)
      .foreach{case(topic,parts)=>
        val topicpath=s"""${path}/${topic}"""
        if(ZookeeperUtil.isExist(zkClient, topicpath)){
          parts.foreach{case(_,part)=>
            val partpath=s"""${topicpath}/${part}"""
            if(ZookeeperUtil.isExist(zkClient, partpath)){
              map += TopicAndPartition(topic,part) -> ZookeeperUtil.readData(zkClient, partpath).toLong
            }else {
              LOG.error("Path is not exist : "+ partpath)
            }
          }
        }else {
          LOG.error("Path is not exist : "+ topicpath)
        }
      }
      map.toMap
    } else {
      LOG.error("文件目录不存在 " + path)
      Map.empty
    }

  }
  /**
   * 功能：获取某些topic的offset
   */
  def getTopicLastOffset(
    kafkaParams: Map[String, String],
    zk: String,
    topics: Set[String]) = {
    val zkClient = ZookeeperUtil.getzkClient(zk)
    val util = new KafkaUtil(kafkaParams)
    util.getLatestLeaderOffsets(topics, zkClient)
  }
  /**
   * 功能：获取所有的topic name
   */
  def getAllTopics(
    kafkaParams: Map[String, String],
    zk: String) = {
    val zkClient = ZookeeperUtil.getzkClient(zk)
    val util = new KafkaUtil(kafkaParams)
    util.getAlltopics(zkClient)
  }
}