package com.atguigu.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 19:28 2020/8/19 
 * @描述 ：
 */
trait BaseApp {
  var appName : String
  def doSomething(ssc: StreamingContext): Unit;
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(5))

    doSomething(ssc)
    ssc.start()
    ssc.awaitTermination()
  }

}
