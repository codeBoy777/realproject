package com.atguigu.realtime.app
import java.io

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.realtime.utils.{ESUtil, KafkaUtil, RedisUtil}
import com.atguigu.util.ConstantUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.eclipse.jetty.io.ssl.ALPNProcessor.Client
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 10:17 2020/8/24
 * @描述 ：
 */
object SaleApp extends BaseApp {
  override var appName: String = "SaleApp"

  override def doSomething(ssc: StreamingContext) = {
     //获取orderInfo流
    val orderInfoDStream: DStream[OrderInfo] = KafkaUtil.getKafkaStream(ssc, ConstantUtil.ORDER_INFO_TOPIC, "saleApp")
                                                        .map(json => JSON.parseObject(json, classOf[OrderInfo]))
    //获取orderDetail流
    val orderDetailDStream: DStream[OrderDetail] = KafkaUtil.getKafkaStream(ssc, ConstantUtil.ORDER_DETAIL_TOPIC, "saleApp")
                                                            .map(json => JSON.parseObject(json, classOf[OrderDetail]))

    //获取user_info流
    val userDStream: DStream[UserInfo] = KafkaUtil.getKafkaStream(ssc, ConstantUtil.ORDER_USER_TOPIC, "saleApp")
                                                  .map(json => JSON.parseObject(json, classOf[UserInfo]))
    //将流转换成k-v形式
    val orderInfoDStreamKV: DStream[(String, OrderInfo)] = orderInfoDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailDStreamKV: DStream[(String, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))
    //对order_info和order_detail合并
    val orderInfoAndOrderDetail: DStream[SaleDetail] = fullJoin(orderInfoDStreamKV, orderDetailDStreamKV)
    //再次对user表进行合并
    val lastResult: DStream[SaleDetail] = fullJoinUser(orderInfoAndOrderDetail, ssc)
    lastResult.foreachRDD(rdd => rdd.foreach(println))
    saleInfoToEs(lastResult)

  }
  //缓存orderInfo到redis
  def cacheOrderInfo(client: Jedis,key : String,orderInfo: OrderInfo) ={
    implicit val f = org.json4s.DefaultFormats
    client.setex(key,30*60,Serialization.write(orderInfo))
  }
  //将三表join的数据存入到es中
  def saleInfoToEs(saleDetail : DStream[SaleDetail])={
    saleDetail.foreachRDD(saleInfo =>{
      saleInfo.foreachPartition(saleIt =>{
        ESUtil.insertBulk("gmall_sale_detail",saleIt.map(sale =>(sale.order_id+":"+sale.order_detail_id,sale)))

      })
    })
  }

  //与用户表进行合合并
  def fullJoinUser(orderInfoAndOrderDetail: DStream[SaleDetail],ssc :StreamingContext) = {
    val spark: SparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    def readUserInfos(ids :String) ={
      spark.read.format("jdbc").option("query",s"select * from user_info where id in (${ids})")
        .option("url","jdbc:mysql://hadoop102:3306/realgmall?userSSL=false")
        .option("user","root")
        .option("password","123456")
        .load
        .as[UserInfo]
        .rdd
    }
    //1.先获取orderInfoAndOrderDetail所有的用户id，传输给readUserInfos函数，得到所对应的user_info，然后经行合并操作
    val nextOrderInfoAndDetail: DStream[(String, SaleDetail)] = orderInfoAndOrderDetail.map(orderInfoAndDetail => (orderInfoAndDetail.user_id, orderInfoAndDetail))
    val orderInfoAndDetailAndUser: DStream[SaleDetail] = nextOrderInfoAndDetail.transform(rdd => {
      //nextOrderInfoAndDetail.cache()
      val ids: String = rdd.map(_._2.user_id).collect().mkString("'","','", "'")
      val userInfoRdd: RDD[(String, UserInfo)] = readUserInfos(ids).map(user => (user.id, user))
      rdd.join(userInfoRdd).map {
        case (_, (rdd, userInfo: UserInfo)) =>
          rdd.mergeUserInfo(userInfo)
      }
    })
    orderInfoAndDetailAndUser

  }

  def fullJoin(DStreamKVOne :DStream[(String,OrderInfo)] ,DStreamKVTwo : DStream[(String,OrderDetail)]) = {
    val saleDetailInfo: DStream[SaleDetail] = DStreamKVOne.fullOuterJoin(DStreamKVTwo).mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
      //建立redis连接
      val client: Jedis = RedisUtil.getJedisClient
      val saleDetails1: Iterator[SaleDetail] = it.flatMap {
        //两个流数据同时到达
        case (orderId, (Some(orderInfo: OrderInfo), Some(orderDetail))) =>
          //1.将order_info缓存到redis
          cacheOrderInfo(client, s"order_info:${orderInfo.id}", orderInfo)
          //2.合并order_info和order_detail
          val saleDetail: SaleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          //3.由于order_info和order_detail是一对多的关系，可能redis缓存中还有和order_info相关的order_detail数据，故还有查找一次redis缓存，如果查找到，合并然后删除
          val saleDetails: mutable.Set[SaleDetail] = client.keys(s"order_detail:${orderDetail.order_id}*").asScala.map(key => {
            val orderDetailJson: String = client.get(key)
            val orderDetailFromRedis: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            //删除redis中缓存的数据
            client.del(key)
            //将其合并
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetailFromRedis)
          })
          saleDetails += saleDetail
          saleDetails
        //order_detail滞后到达
        case (orderId, (Some(orderInfo: OrderInfo), None)) =>
          //1.把orderInfo信息写入到redis缓存，以便后面没到的order_detail下次到可以继续和order_info合并
          cacheOrderInfo(client, s"order_info:${orderInfo.id}", orderInfo)
          //2.从redis缓存读取order_detail信息进行合并，并且将其从缓存中删除
          val saleDetails: mutable.Set[SaleDetail] = client.keys(s"order_detail:${orderInfo.id}*").asScala.map(key => {
            val orderDetailJson: String = client.get(key)
            val orderDetailFromRedis: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            client.del(key)
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetailFromRedis)
          })
          saleDetails
        //order_info数据滞后到达
        case (orderId, (None, Some(orderDetail))) =>
          //1.先去缓存中找到相应的order_info信息进行合并
          val orderInfo: String = client.get(s"order_info:${orderDetail.order_id}")
          if (orderInfo != null) {
            val orderInfoFromRedis: OrderInfo = JSON.parseObject(orderInfo, classOf[OrderInfo])
            SaleDetail().mergeOrderInfo(orderInfoFromRedis).mergeOrderDetail(orderDetail) ::Nil
          } else {
            //如果再redis缓存没拿到数据，就要将这个order_detail缓存起来供后续来的order_info进行合并
            implicit val f = org.json4s.DefaultFormats
            client.setex(s"order_detail:${orderDetail.order_id}:${orderDetail.id}", 30 * 60, Serialization.write(orderDetail))
            Nil
          }
      }
      client.close()
      saleDetails1
    })
    //saleDetailInfo.foreachRDD(res => res.foreach(println))
    saleDetailInfo
  }
}
