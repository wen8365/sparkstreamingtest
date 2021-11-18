package wd.demo.wbskafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import wd.demo.wbskafka.service.KafkaComsumer;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        //将Kafka消费者实例化,运行起来....
        KafkaComsumer kafkaComsumer = new KafkaComsumer();
        kafkaComsumer.start();
        SpringApplication.run(Application.class, args);
    }
}
