package com.csot.kafka;

import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessagingException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

@Component
public class AligndataConsumer {
    private final static Logger logger = LoggerFactory.getLogger(AligndataConsumer.class);
    @Autowired
    private HttpPostSender httpPostSender;
    @KafkaListener(topics = "aligndatanew",groupId = "cassandranew")
    public void listen(List<String> aligns) throws MessagingException, ParseException {
        for (String align : aligns) {
            logger.info("receive message:{}",align);
            //将json字符串解析成json对象的形式 JSONObject jsonObject = JSONObject.from(line);
            JSONObject json = JSONObject.parseObject(align);
            httpPostSender.send(json);
        }
    }
}
