package com.csot.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessagingException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

@Component
public class ScopedataConsumer {

    @Value("${json.file.path_scope01}")
    private String jsonFilePath01;
    @Value("${json.file.path_scope02}")
    private String jsonFilePath02;
    @Autowired
    private HttpPostSenderScope httpPostSender;
    // 记录当前应该存储到哪个文件的标记
    private boolean writeToFirstFile = true;
    @KafkaListener(topics = {"scopedata"},groupId = "cassandranew")
    public void listen(List<String> scopes) throws MessagingException,ParseException{
        // 根据标记判断存储文件路径
        String jsonFilePath = writeToFirstFile ? jsonFilePath01 : jsonFilePath02;
        saveDataToJson(scopes, jsonFilePath);
        // 文件写入完成后通知HttpPostSender上传文件
        try {
            httpPostSender.uploadHandCheckFile(jsonFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 切换标记，以便下一次存储到另一个文件
        writeToFirstFile = !writeToFirstFile;

    }

    private void saveDataToJson(List<String> data, String filePath) {
        // 创建 JSON 数组字符串
        StringBuilder jsonArrayBuilder = new StringBuilder();
        jsonArrayBuilder.append("[");
        for (String hand : data) {
            System.out.println("scope传输");
            // 假设每条数据都是一个 JSON 对象字符串
            jsonArrayBuilder.append(hand).append(",");
        }
        if (!data.isEmpty()) {
            jsonArrayBuilder.deleteCharAt(jsonArrayBuilder.length() - 1); // 删除最后一个逗号
        }
        jsonArrayBuilder.append("]");

        // 将 JSON 数组字符串写入文件
        try (FileWriter fileWriter = new FileWriter(filePath, true)) {
            fileWriter.write(jsonArrayBuilder.toString());
            fileWriter.write("\n"); // 换行
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 每隔10分钟执行一次定时任务，用于切换存储文件路径
    @Scheduled(fixedDelay = 60000)
    public void switchFilePath() {
        writeToFirstFile = !writeToFirstFile;
    }

}
