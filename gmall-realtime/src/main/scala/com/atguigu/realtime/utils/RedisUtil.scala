package com.atguigu.realtime.utils

import redis.clients.jedis.Jedis


/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 19:37 2020/8/18 
 * @描述 ：
 */
object RedisUtil {
  val host = PropertiesUtil.getProperties("redis.host")
  val port = PropertiesUtil.getProperties("redis.port").toInt

  //获得一个redis的连接
  def getJedisClient: Jedis = new Jedis(host, port)
}
