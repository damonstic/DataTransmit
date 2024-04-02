package com.csot.kafka;

import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessagingException;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.List;

@Component
public class HandCheckConsumer {
    private final static Logger logger = LoggerFactory.getLogger(HandCheckConsumer.class);
    @Autowired
    private HttpPostSender httpPostSender;
    @KafkaListener(topics = {"handchecknew"},groupId = "cassandranew")
    public void listen(List<String> hands) throws MessagingException, ParseException {
        for (String hand : hands) {
            logger.info("handchecknew:{}",hand);
            JSONObject json = JSONObject.parseObject(hand);
            httpPostSender.send(json);

        }

    }
}
