package com.atguigu.realtime.app

import java.lang

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.StartUpLog
import com.atguigu.realtime.utils.{KafkaUtil, RedisUtil}
import com.atguigu.util.ConstantUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 19:42 2020/8/18 
 * @描述 ：
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream: DStream[String] = KafkaUtil.getKafkaStream(ssc, ConstantUtil.START_LOG_TOPIC,"dauApp")
    val startupLogStream: DStream[StartUpLog] = sourceStream.map(jsonLog => JSON.parseObject(jsonLog, classOf[StartUpLog]))
    startupLogStream.print()
    //去重，一个分区建立一个redis
    val filteredStartupLogStream: DStream[StartUpLog] = startupLogStream.mapPartitions(startupLogIt => {
      val client: Jedis = RedisUtil.getJedisClient
      val res: Iterator[StartUpLog] = startupLogIt.filter(log => {
        val key = s"mids:${log.logDate}"
        val r: lang.Long = client.sadd(key, log.mid)
        r == 1
      })
      client.close()
      res
    })
//{"logType":"startup","area":"shanghai","uid":"3934","os":"android","appId":"gmall","channel":"huawei","mid":"mid_207","version":"1.2.0","ts":1597824416526}
    //把数据写到hbase
    import org.apache.phoenix.spark._
    filteredStartupLogStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION","TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
      )
    })
    ssc.start()
    ssc.awaitTermination()

  }

}
