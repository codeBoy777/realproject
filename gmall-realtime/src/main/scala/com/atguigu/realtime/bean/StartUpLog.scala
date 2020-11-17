package com.atguigu.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 19:30 2020/8/18 
 * @描述 ：
 */
case class StartUpLog(mid: String,
                        uid: String,
                        appId: String,
                        area: String,
                        os: String,
                        channel: String,
                        logType: String,
                        version: String,
                        ts: Long,
                        var logDate: String = null,
                        var logHour: String = null){
  private val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)

}
