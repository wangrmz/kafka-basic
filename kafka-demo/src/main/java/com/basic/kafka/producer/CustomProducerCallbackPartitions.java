package com.basic.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallbackPartitions {

    public static void main(String[] args) throws InterruptedException {

        // 0 配置
        Properties properties = new Properties();

        // 连接集群 bootstrap.servers
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 指定对应的key和value的序列化类型 key.serializer
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 默认分区策略

        /**
         * （1）指明partition的情况下，直
         * 接将指明的值作为partition值；
         * 例如partition=0，所有数据写入
         * 分区0
         *
         * （2）没有指明partition值但有key的情况下，将key的hash值与topic的
         * partition数进行取余得到partition值；
         * 例如：key1的hash值=5， key2的hash值=6 ，topic的partition数=2，那
         * 么key1 对应的value1写入1号分区，key2对应的value2写入0号分区。
         *
         *
         * （3）既没有partition值又没有key值的情况下，Kafka采用Sticky Partition（黏性分区器），会随机选择一个分区，并尽可能一直
         * 使用该分区，待该分区的batch已满或者已完成，Kafka再随机一个分区进行使用（和上一次的分区不同）。
         * 例如：第一次随机选择0号分区，等0号分区当前批次满了（默认16k）或者linger.ms设置的时间到， Kafka再随机一个分区进
         * 行使用（如果还是0会继续随机）。
         *
         */

        // 比如业务希望发向指定分区，订单业务可以使用订单表名作为key，采用key的hash值对分区数取余，这样数据就发向指定分区了


        // 关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.kafka.producer.MyPartitioner");

        // 1 创建kafka生产者对象
        // "" hello
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2 发送数据
        for (int i = 0; i < 5; i++) {
            // 分区规则
            kafkaProducer.send(new ProducerRecord<>("first", "hello111" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null) {
                        System.out.println("主题： " + metadata.topic() + " 分区： " + metadata.partition());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

            Thread.sleep(2);
        }

        // 3 关闭资源
        kafkaProducer.close();
    }
}
