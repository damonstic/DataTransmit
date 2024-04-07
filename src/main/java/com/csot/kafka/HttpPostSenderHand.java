package com.csot.kafka;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.*;


@Component
public class HttpPostSenderHand {
    private final static Logger logger = LoggerFactory.getLogger(HttpPostSenderHand.class);
    private final RestTemplate restTemplate;

    @Value("${endpoint.url}")
    private String endpointUrl;

    @Autowired
    public HttpPostSenderHand(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void uploadHandCheckFile(String filePath) throws IOException {
        try {
            // 读取文件内容
            JSONArray jsonArray = readJsonFile(filePath);

            // 如果 JSON 数据不为空，则发送数据
            if (jsonArray != null && !jsonArray.isEmpty()) {
                send("hand", jsonArray);
                logger.info("发送hand");
                // 清空文件内容或删除文件
                clearJsonFile(filePath);
            }
        } catch (IOException e) {
            logger.error("Error reading JSON file: " + e.getMessage());
        }
    }


    // 读取 JSON 文件内容的方法
    private JSONArray readJsonFile(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            StringBuilder jsonData = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonData.append(line);
            }
            return JSON.parseArray(jsonData.toString());
        }
    }

    private void clearJsonFile(String filePath) throws IOException {
        try (FileWriter fileWriter = new FileWriter(filePath)) {
            // 清空文件内容
            fileWriter.write("");
        } catch (IOException e) {
            // 处理异常
            e.printStackTrace();
        }
    }


    public void send(String dataType,JSONArray inputData) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8); //创建HTTP头部信息

        //传参
        JSONObject requestData = new JSONObject();
        requestData.put("forecast_length", "24");
        requestData.put("interval", "10min");
        requestData.put("train_test_cutoff", "2024-04-03 00:00:00.000");
        requestData.put("n_training_days", "2");
        requestData.put("upper_limit", "0.95");
        requestData.put("lower_limit", "0.05");
        requestData.put("data_type", dataType);
        requestData.put("input_data", inputData);

        HttpEntity<String> requestEntity = new HttpEntity<>(requestData.toString(), headers);//创建HTTP实体对象
        ResponseEntity<String> response = restTemplate.exchange(endpointUrl, HttpMethod.POST, requestEntity, String.class);//发送HTTP请求
        logger.info("HTTP Status Code: " + response.getStatusCode()); //处理HTTP响应
        logger.info("Response Body: " + response.getBody());
    }

}
