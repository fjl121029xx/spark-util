# 用于获取kafka每个topic的偏移量并记录至zk
scala version 2.10
spark version 1.6.0
kafka version 0.8

* 
# Example 

```
  def main(args: Array[String]): Unit = {
  val groupid="kafkatopicoffset"
  val day="20180115"
  var kafkaParams = Map[String, String](
      "metadata.broker.list" ->broker ,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> groupid)
  //write offset to zk
  KafkaOffsetRecord.recordKafkaOffset(kafkaParams, groupid, day, zk, Set("test"))
  //read offset from zk 
  KafkaOffsetRecord.getKafkaOffset(kafkaParams, groupid, day, zk, broker, Set("test"))
  }
  
```