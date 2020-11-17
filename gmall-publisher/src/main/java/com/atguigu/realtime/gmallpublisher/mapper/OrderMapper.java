package com.atguigu.realtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 20:22 2020/8/19
 * @描述 ：
 */
public interface OrderMapper {
    Double getTotalAmount(String date);

    List<Map<String, Object>> getHourAmount(String date);




}
