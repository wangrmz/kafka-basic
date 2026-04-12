package com.basic.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 定义一个生产者
 */

public class CustomProducer {

    public static void main(String[] args) {

        // 0 配置
        Properties properties = new Properties();

        // 连接集群 bootstrap.servers
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");

//        ➜  bin git:(master) /opt/homebrew/bin/kafka-topics --topic test-topic   --describe --bootstrap-server localhost:9092
//        Topic: test-topic	TopicId: _t7b5nmyTA-DH0wMeCyYuA	PartitionCount: 1	ReplicationFactor: 1	Configs: segment.bytes=1073741824
//        Topic: test-topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
//➜  bin git:(master)

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 这里必须指定
        // 指定对应的key和value的序列化类型 key.serializer
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 1 创建kafka生产者对象
        // "" hello
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2 发送数据
        for (int i = 0; i < 5; i++) {
            // 指定要发送的主题，发送的消息
            kafkaProducer.send(new ProducerRecord<>("test-topic", "2026.04.12测试发送消息" + i));
            System.out.println("发送了消息");
        }

        // 3 关闭资源
        kafkaProducer.close();
    }
}
