package com.csot.kafka;

import com.alibaba.fastjson2.JSONObject;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.List;

//@Component
public class TacttimeConsumer {
    @KafkaListener(topics = {"tacttimenew"},groupId = "cassandranew")
    public void listen(List<String> tacts) throws ParseException {
        for (String tact : tacts) {
            System.out.println(JSONObject.parseObject(tact));
        }

    }
}
