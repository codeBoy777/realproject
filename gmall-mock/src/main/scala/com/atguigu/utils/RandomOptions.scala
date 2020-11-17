package com.atguigu.utils

import scala.collection.mutable.ListBuffer

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 15:02 2020/8/17 
 * @描述 ：
 */
object RandomOptions {
  def apply[T](opts: (T, Int)*) = {
    val randomOptions = new RandomOptions[T]()
    // randomOptions.totalWeight = (0 /: opts) (_ + _._2) // 计算出来总的比重
    randomOptions.totalWeight = opts.foldLeft(0)(_+_._2)
    opts.foreach {
      case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)
    }
    randomOptions
  }
}

class RandomOptions[T] {
  var totalWeight: Int = _
  var options = ListBuffer[T]()

  /**
   * 获取随机的 Option 的值
   *
   * @return
   */
  def getRandomOption() = {
    options(RandomNumUtil.randomInt(0, totalWeight - 1))
  }

}
