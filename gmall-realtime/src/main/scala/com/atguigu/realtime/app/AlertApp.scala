package com.atguigu.realtime.app
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.realtime.utils.{ESUtil, KafkaUtil}
import com.atguigu.util.ConstantUtil
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.util.control.Breaks.{break, breakable}

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 11:25 2020/8/21 
 * @描述 ：
 */
object AlertApp extends BaseApp {
  override var appName: String = "AlertApp"
  //消费topic中的数据
  override def doSomething(ssc: StreamingContext): Unit = {
    //获取kafka流
    val sourceDStream: DStream[String] = KafkaUtil.getKafkaStream(ssc, ConstantUtil.EVENT_LOG_TOPIC,"alertApp").window(Minutes(5),Seconds(5))
      //将json字符串转换为json对象并且按mid进行分组
      val eventDStream: DStream[(String, Iterable[EventLog])] = sourceDStream.map(json => {
        JSON.parseObject(json, classOf[EventLog])
      }).map(log => log.mid -> log).groupByKey
    eventDStream.foreachRDD(rdd => rdd.foreach(println))
    val alertDStream: DStream[(Boolean, AlertInfo)] = eventDStream.map {
      case (mid, it: Iterable[EventLog]) =>
        //用于存放用户id
        val uids = new util.HashSet[String]()

        //用于保存5分钟内所有的事件
        val items = new util.HashSet[String]()

        //用于存放事件id
        val events = new util.ArrayList[String]()
        //用于记录是否点击了
        var isClicked = false

        breakable {
          it.foreach(res => {
            events.add(res.eventId)
            res.eventId match {
              case "coupon" =>
                uids.add(res.uid)
                items.add(res.itemId)
              case "clickItem" =>
                isClicked = true
                break
              case _ =>
            }
          })
        }
        (uids.size() >= 3 && isClicked, AlertInfo(mid, uids, items, events, System.currentTimeMillis()))
    }

    //alertDStream.foreachRDD(rdd =>rdd.foreach(println))
    alertDStream.filter(_._1)
      .map(_._2)
      .foreachRDD(res =>{
        res.foreachPartition((infoIt: Iterator[AlertInfo]) => {
          // 每分钟只记录一次预警   id:  mid_分钟
          ESUtil.insertBulk("gmall_coupon_alert", infoIt.map(info => (s"${info.mid}:${System.currentTimeMillis()/1000/60}", info)))
        })
      })



  }

}
