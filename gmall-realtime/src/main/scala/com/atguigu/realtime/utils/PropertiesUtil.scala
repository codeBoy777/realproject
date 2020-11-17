package com.atguigu.realtime.utils

import java.io.InputStream
import java.util.Properties

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 17:50 2020/8/18 
 * @描述 ：
 */
object PropertiesUtil {
  //获取配置文件输入流
  private val inputStream: InputStream = this.getClass.getClassLoader.getResourceAsStream("config.properties")
  //配置文件对象
  private val properties = new Properties()
  properties.load(inputStream)
  //获取配置文件信息方法
  def getProperties(propertyName : String) : String={
    val propertyVal: String = properties.getProperty(propertyName)
    propertyVal

  }
  //测试方法
  /*def main(args: Array[String]): Unit = {
    println(getProperties("kafka.bootstrap.servers"))
  }*/

}
