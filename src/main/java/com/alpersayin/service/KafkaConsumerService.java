package com.alpersayin.service;

import com.alpersayin.thread.KafkaConsumerThreadHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class KafkaConsumerService {

    private final KafkaConsumer<String, String> consumer;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ExecutorService executor;

    public KafkaConsumerService() {
        Properties prop = createConsumerConfig("test-group-init");
        this.consumer = new KafkaConsumer<>(prop);
    }

    public void executeByTopicName(int numberOfThreads, String topicName) {

        executor = new ThreadPoolExecutor(
                numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());

        Long totalCount = count(topicName);

        try {

            this.consumer.subscribe(Collections.singletonList(topicName));

            while (!stopped.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    executor.submit(
                            new KafkaConsumerThreadHandler(record, totalCount, consumer)
                    );
                }
            }

        } catch (WakeupException we) {
            if (!stopped.get()) stopped.set(true);
        } finally {
            assert consumer != null;
            shutdown();
        }

    }

    public void shutdown() {
        if (consumer != null) {
            consumer.close();
            log.warn("Consumer closed.");
        }
        if (executor != null) {
            executor.shutdown();
            log.warn("Executor closed.");
        }
        try {
            assert executor != null;
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out
                        .println("Timed out waiting for consumer threads to shut down.");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown.");
        }
    }

    public Long count(String topicName) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createConsumerConfig("test-group-1"));
        consumer.subscribe(Collections.singletonList(topicName));

        Set<TopicPartition> assignment;
        while ((assignment = consumer.assignment()).isEmpty()) {
            consumer.poll(Duration.ofMillis(100));
        }
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
        final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(assignment);
        assert (endOffsets.size() == beginningOffsets.size());
        assert (endOffsets.keySet().equals(beginningOffsets.keySet()));

        Long totalCount = beginningOffsets.entrySet().stream().mapToLong(entry -> {
            TopicPartition tp = entry.getKey();
            Long beginningOffset = entry.getValue();
            Long endOffset = endOffsets.get(tp);
            return endOffset - beginningOffset;
        }).sum();

        consumer.close();
        log.warn("Count consumer closed.");
        return totalCount;
    }

    private static Properties createConsumerConfig(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "http://host.docker.internal:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }


}
