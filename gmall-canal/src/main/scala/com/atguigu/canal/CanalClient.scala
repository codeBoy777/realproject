package com.atguigu.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.atguigu.util.ConstantUtil
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 11:31 2020/8/19 
 * @描述 ：
 */
object CanalClient {
  //将rowDatas上传到kafka
  def sendToKafka(topic: String, JSONString: String) = {
    MyKafkaUtil.send(topic,JSONString)
  }

  //处理多行rowDatas,并将rowData依次传入到kafka
  def handleDiffData(topic : String,rowDatas: util.List[CanalEntry.RowData])={
    //迭代处理rowDatas里面的多行数据
    for(rowData <- rowDatas.asScala){
      //创建json对象用于保存处理之后的k-v对
      val JSONObj = new JSONObject()
      //得到更新的所有的列
      val columns: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
      for(column <- columns.asScala){
        val name: String = column.getName
        val value: String = column.getValue
        //将处理的数据保存到json对象中
        JSONObj.put(name,value)
      }
      //将第一行处理的结果发送到kafka
      new Thread(){
        override def run(): Unit ={
          Thread.sleep(new Random().nextInt(2 * 1000))
          sendToKafka(topic,JSONObj.toJSONString)
        }
      }.start()
    }
  }
  def handleRowDatas(rowDatas: util.List[CanalEntry.RowData], tableName: String, eventType: CanalEntry.EventType) = {
    if(tableName == "order_info" && eventType == EventType.INSERT &&rowDatas != null){
      handleDiffData(ConstantUtil.ORDER_INFO_TOPIC,rowDatas)
    }else if(tableName == "order_detail" && eventType == EventType.INSERT && rowDatas != null && !rowDatas.isEmpty){
      handleDiffData(ConstantUtil.ORDER_DETAIL_TOPIC,rowDatas)
    }/*else if(tableName=="user_info" &&eventType ==EventType.INSERT &&rowDatas != null && !rowDatas.isEmpty){
      handleDiffData()
    }*/
  }

  def main(args: Array[String]): Unit = {
    //创建连接canal连接器对象
    val connector: CanalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "")
    //连接canal
    connector.connect()
    //监控mysql中所有的表
    connector.subscribe("realgmall.*")
    //解析数据
    while (true){
      //获取数据，一个消息对应多条sql语句的执行
      val msg: Message = connector.get(100)
      val entries: util.List[CanalEntry.Entry] = msg.getEntries
      //println(msg)
      if(entries != null && entries.size()>0){
        for(entry <- entries.asScala){
          //entry => storeValue => col
          if(entry != null &&entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA){
            //获取storeValue对象
            val storeValue: ByteString = entry.getStoreValue
            //获取rowChange对象
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            //得到rowDatas,具体操作变更数据，可为多条
            val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            
            //处理得到的数据
            handleRowDatas(rowDatas,entry.getHeader.getTableName,rowChange.getEventType)

          }
        }

      }
    }


  }

}
