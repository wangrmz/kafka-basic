package com.basic.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomConsumer {

    public static void main(String[] args) {

        // 0 配置
        Properties properties = new Properties();

        // 连接 bootstrap.servers
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test5");

        // 设置分区分配策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.StickyAssignor");

        // 配置自动重置偏移量（首次消费或偏移量不存在时）
        // earliest: 从最早的记录开始消费
        // latest: 从最新的记录开始消费（默认值）
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 1 创建一个消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 2 订阅主题 first
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        topics.add("test-topic");
        kafkaConsumer.subscribe(topics);

//        ConsumerRecord(topic = test-topic, partition = 0, leaderEpoch = 0, offset = 18, CreateTime = 1775980842656, serialized key size = -1, serialized value size = 29, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 2026.04.12测试发送消息0)
//        ConsumerRecord(topic = test-topic, partition = 0, leaderEpoch = 0, offset = 19, CreateTime = 1775980842662, serialized key size = -1, serialized value size = 29, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 2026.04.12测试发送消息1)
//        ConsumerRecord(topic = test-topic, partition = 0, leaderEpoch = 0, offset = 20, CreateTime = 1775980842662, serialized key size = -1, serialized value size = 29, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 2026.04.12测试发送消息2)
//        ConsumerRecord(topic = test-topic, partition = 0, leaderEpoch = 0, offset = 21, CreateTime = 1775980842662, serialized key size = -1, serialized value size = 29, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 2026.04.12测试发送消息3)
//        ConsumerRecord(topic = test-topic, partition = 0, leaderEpoch = 0, offset = 22, CreateTime = 1775980842662, serialized key size = -1, serialized value size = 29, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 2026.04.12测试发送消息4)


        // 3 消费数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

            if (!consumerRecords.isEmpty()) {
                kafkaConsumer.commitAsync();
            }
        }
    }
}
