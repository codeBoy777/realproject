package com.atguigu.realtime.gmallpublisher.service;

import java.util.Map;

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 21:30 2020/8/18
 * @描述 ：
 */
public interface PublisherService {
    long getDau(String date);

    Map<String, Long>  getHourDau(String date);

    Double getTotalAmount(String date);


    Map<String, Double> getHourAmount(String date);



}
