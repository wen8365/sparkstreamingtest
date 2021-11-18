package wd.demo.wbskafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Service
public class KafkaComsumer extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaComsumer.class);
    private KafkaConsumer<String, String> consumer;
    private final static String topic = "sparkstreaming";
    @Override
    public void run() {
        //加载kafka消费者参数
        Properties props = new Properties();
        props.put("bootstrap.servers", "master1:9092");
        props.put("group.id", "kafka");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "15000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者对象
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        //死循环，持续消费kafka
        while (true) {
            try {
                //消费数据，并设置超时时间
                ConsumerRecords<String, String> records = consumer.poll(100);
                //Consumer message
                for (ConsumerRecord<String, String> record : records) {
                    //Send message to every client
                    for (WebSocket webSocket : WebSocket.webSocketSet) {
                        webSocket.sendMessage(record.value());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("send message error:", e);
            }
        }
    }

    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            LOGGER.error("close error:", e);
        }
    }

    //供测试用，若通过tomcat启动需通过其他方法启动线程
    public static void main(String[] args) {
        KafkaComsumer kafkaComsumer = new KafkaComsumer();
        kafkaComsumer.start();
    }
}
