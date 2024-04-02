package com.csot.kafka;

import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;


@Component
public class HttpPostSender {
    private final static Logger logger = LoggerFactory.getLogger(HttpPostSender.class);
    private final RestTemplate restTemplate;

    @Value("${endpoint.url}")
    private String endpointUrl;

    @Autowired
    public HttpPostSender(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }
    public void send(JSONObject json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8); //创建HTTP头部信息

        HttpEntity<String> requestEntity = new HttpEntity<>(json.toString(), headers);//创建HTTP实体对象

        ResponseEntity<String> response = restTemplate.exchange(endpointUrl, HttpMethod.POST, requestEntity, String.class);//发送HTTP请求

        logger.info("HTTP Status Code: " + response.getStatusCode()); //处理HTTP响应
        logger.info("Response Body: " + response.getBody());
    }
}
