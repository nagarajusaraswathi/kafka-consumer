package com.techprimers.kafka.springbootkafkaconsumerexample.listener;

import com.techprimers.kafka.springbootkafkaconsumerexample.config.KafkaConfiguration;
import com.techprimers.kafka.springbootkafkaconsumerexample.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;


@Service
public class KafkaConsumer {
    Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Value("${kafka.topic}")
     private   String topicName;
    @Value("${file.Location}")
    private String fileLocation;


    @KafkaListener(topics = "${kafka.topic}", groupId = "group_id")
    public void consume(String message, Acknowledgment acknowledgment) throws IOException {
        logger.info("topicName ::"+topicName);
        logger.info("fileLocation ::"+fileLocation);
        String s = UUID.randomUUID().toString();
       File file = new File(fileLocation+"/"+s+".txt");
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(message);
        fileWriter.flush();;
        fileWriter.close();
        System.out.println("Consumed message: " + message);
        acknowledgment.acknowledge();
    }


    @KafkaListener(topics = "Kafka_Example_json", groupId = "group_json",
            containerFactory = "userKafkaListenerFactory")
    public void consumeJson(User user) {

        System.out.println("Consumed JSON Message: " + user);
    }
}
