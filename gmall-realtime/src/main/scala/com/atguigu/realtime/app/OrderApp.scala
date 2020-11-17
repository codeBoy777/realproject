package com.atguigu.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.OrderInfo
import com.atguigu.realtime.utils.KafkaUtil
import com.atguigu.util.ConstantUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 19:34 2020/8/19 
 * @描述 ：
 */
object OrderApp extends BaseApp {
  override var appName: String = "OrderApp"

  override def doSomething(ssc: StreamingContext): Unit = {
    val sourceStream: DStream[String] = KafkaUtil.getKafkaStream(ssc, ConstantUtil.ORDER_INFO_TOPIC,"orderApp")
    val orderInfoStream: DStream[OrderInfo] = sourceStream.map(json => JSON.parseObject(json, classOf[OrderInfo]))

    import org.apache.phoenix.spark._
    orderInfoStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix("GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
      )
    })
  }

}
