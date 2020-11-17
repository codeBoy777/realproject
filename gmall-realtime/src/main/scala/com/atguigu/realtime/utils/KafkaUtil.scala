package com.atguigu.realtime.utils


import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 18:14 2020/8/18 
 * @描述 ：
 */
object KafkaUtil {
  var kafkaParams = Map[String,Object](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 从kafka最新的位置开始消费.
    "auto.offset.reset" -> "latest",
    // offset是否自己动提交
    "enable.auto.commit" -> (true: java.lang.Boolean)
    )
  def getKafkaStream(ssc: StreamingContext, topic: String,groupId : String) = {
    kafkaParams += "group.id" -> groupId
    KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set(topic),kafkaParams)
    ).map(_.value())
  }
}