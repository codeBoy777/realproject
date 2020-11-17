package com.atguigu.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 11:24 2020/8/21 
 * @描述 ：
 */
case class EventLog(
                     mid: String,
                     uid: String,
                     appId: String,
                     area: String,
                     os: String,
                     logType: String,
                     eventId: String,
                     pageId: String,
                     nextPageId: String,
                     itemId: String,
                     ts: Long,
                     var logDate: String = null,
                     var logHour: String = null
                   ){
  val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)

}
