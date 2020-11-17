package com.atguigu.realtime.gmallpublisher.mapper;


import java.util.List;
import java.util.Map;

/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 21:24 2020/8/18
 * @描述 ：
 */
public interface DauMapper {
    //查询日活总数
    long getDauTotal(String date);
    //查询小时明细
    List<Map<String,Object>> getHourDau(String date);

}
