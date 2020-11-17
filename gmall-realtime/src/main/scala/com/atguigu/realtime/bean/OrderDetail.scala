package com.atguigu.realtime.bean

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 11:08 2020/8/24 
 * @描述 ：
 */
case class OrderDetail(
                        id: String,
                        order_id: String,
                        sku_name: String,
                        sku_id: String,
                        order_price: String,
                        img_url: String,
                        sku_num: String
                      )
