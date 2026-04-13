package integration.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author wrmeng
 * @create 2026-04-12 -16:27
 * @Description: Flink 生产者
 **/
public class FlinkKafkaProducer1 {


    /**
     * Flink 是什么？
     * 一句话理解：Flink 是一个真正的实时流处理引擎，可以对源源不断的数据（比如 Kafka 里的消息）进行实时计算，毫秒级输出结果。
     * 打个比方：
     * Hadoop/Spark 像是等收集满一筐水果再一起洗（批处理）
     * Flink 像是来一个水果就洗一个，洗完马上装篮（流处理）
     * Flink 的核心能力是：低延迟 + 高吞吐 + 状态管理 + Exactly-Once 语义，而且与 Kafka 有非常紧密的生态集成
     *
     *  技术	                        处理模式	                延迟	                典型用途
        Hadoop/Spark	    批处理（一次处理一批数据）	        分钟~小时	    离线报表、数据仓库
        Flink	            真正的流处理（来一条处理一条）	    毫秒级	        实时大屏、实时风控、实时推荐
     */

    public static void main(String[] args) throws Exception {

        // 0 初始化 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 和操作主题的分区数相同最好
        env.setParallelism(3);

        // 1 读取集合中数据
        ArrayList<String> wordsList = new ArrayList<>();
        wordsList.add("hello");
        wordsList.add("world");
        wordsList.add("world1");
        wordsList.add("world2");
        wordsList.add("world3");
        DataStream<String> stream = env.fromCollection(wordsList);
//        DataStreamSource<String> stream = env.fromCollection(wordsList);


        // 2 kafka 生产者配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 3 创建 kafka 生产者
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "first",
                new SimpleStringSchema(),
                properties
        );


        // 4 生产者和 flink 流关联
        stream.addSink(kafkaProducer);

        // 5 执行
        env.execute();
        //
//        flume /opt/homebrew/bin/kafka-console-consumer --topic first --bootstrap-server localhost:9092
//        world
//                hello

    }


}
