package com.alpersayin.thread;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class KafkaConsumerThreadHandler implements Runnable  {

    private final ConsumerRecord<String, String> record;
    Long totalCount;
    KafkaConsumer<String, String> kafkaConsumer;

    public KafkaConsumerThreadHandler(ConsumerRecord<String, String> record,
                                      Long totalCount,
                                      KafkaConsumer<String, String> kafkaConsumer) {
        this.record = record;
        this.totalCount = totalCount;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void run() {

        log.info("Record value  " + record.value() +  " consumed from ThreadID: " + Thread.currentThread().getId());

        if (true) {

            kafkaConsumer.wakeup();
        }
    }

}
