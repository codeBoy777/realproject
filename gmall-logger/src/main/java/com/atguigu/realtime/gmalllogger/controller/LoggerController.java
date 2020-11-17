package com.atguigu.realtime.gmalllogger.controller;

import com.alibaba.fastjson.*;
import com.atguigu.util.ConstantUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
//import com.atguigu.util.ConstantUtil;
/**
 * @author : SqChan
 * @email : SqChan17@163.com
 * @date : 16:54 2020/8/17
 * @描述 ：
 */
@RestController
public class LoggerController {
    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log){

        //日志数据添加时间戳
        //log = addTs(log);
        System.out.println(log);

        //日志数据落盘
        saveToDisk(log);

        //日志数据发送到kafaka
        sendToKafka(log);

        System.out.println(log);
        return log;
       // return "ok";
    }
    @Autowired
    KafkaTemplate<String,String> kafka;
    private void sendToKafka(String log) {
        if(log.contains("startup")){
            kafka.send(ConstantUtil.START_LOG_TOPIC, log);
        }else{
            kafka.send(ConstantUtil.EVENT_LOG_TOPIC, log);
        }

    }

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);
    private void saveToDisk(String log) {
        logger.info(log);

    }

    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts",System.currentTimeMillis());
        return obj.toJSONString();
    }

}
