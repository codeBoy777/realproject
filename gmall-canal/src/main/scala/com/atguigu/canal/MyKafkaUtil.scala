package com.atguigu.canal

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 18:38 2020/8/19 
 * @描述 ：
 */
object MyKafkaUtil {
  private val props = new Properties()
  props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val producer = new KafkaProducer[String, String](props)
  def send(topic :String,message : String) ={
    producer.send(new ProducerRecord[String,String](topic,message))
  }

}
