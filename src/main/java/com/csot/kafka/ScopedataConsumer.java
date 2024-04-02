package com.csot.kafka;

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessagingException;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.List;

@Component
public class ScopedataConsumer {
    @Autowired
    private HttpPostSender httpPostSender;
    @KafkaListener(topics = {"scopedatanew"},groupId = "cassandranew")
    public void listen(List<String> scopes) throws MessagingException, ParseException, JsonProcessingException {
        for (String scope : scopes) {
            JSONObject json = JSONObject.parseObject(scope);
            httpPostSender.send(json);
        }

    }
}
