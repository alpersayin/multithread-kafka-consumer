package com.alpersayin.controller;

import com.alpersayin.service.KafkaConsumerService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@Slf4j
@AllArgsConstructor
@RestController
@RequestMapping("/api")
public class MainController {

    private final KafkaConsumerService kafkaConsumerService;

    @PostMapping("/consume-async")
    public String consumeByTopicName(@RequestParam int numberOfThreads, @RequestParam String topicName) {

        kafkaConsumerService.executeByTopicName(numberOfThreads, topicName);

        return "Consuming completed..";
    }

}
