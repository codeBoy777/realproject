package com.atguigu.realtime.bean

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 11:23 2020/8/21 
 * @描述 ：
 */
case class AlertInfo(
                      mid: String,
                      uids: java.util.HashSet[String],
                      itemIds: java.util.HashSet[String],
                      events: java.util.List[String],
                      ts: Long
                    )
