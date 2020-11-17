package com.atguigu.realtime.utils


import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 20:10 2020/8/23
 * @描述 ：
 */
object ESUtil {
  //获取es客户端对象
  val factory = new JestClientFactory()
  factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
    .maxTotalConnection(100)
    .connTimeout(10000)
    .readTimeout(10000)
    .build())

/*
  def main(args: Array[String]): Unit = {
    insertBulk("urls",List(User("a",22),User("b",21)).toIterator)

}*/
  def insertBulk(index: String, itUsers: Iterator[Object]): Unit = {
    val client: JestClient = factory.getObject()
    val builder: Bulk.Builder = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
    itUsers.foreach{
      case (id :String,source) =>
        val action: Index = new Index.Builder(source)
          .id(id)
          .build()
        builder.addAction(action)
      case source =>
        val action: Index = new Index.Builder(source).build()
        builder.addAction(action)
    }
    client.execute(builder.build())
    client.shutdownClient()


}
}

case class User(name : String,age : Int)
