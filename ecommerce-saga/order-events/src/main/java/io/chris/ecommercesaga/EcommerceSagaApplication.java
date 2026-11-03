package io.chris.ecommercesaga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class EcommerceSagaApplication {

    public static void main(String[] args) {
        SpringApplication.run(EcommerceSagaApplication.class, args);
    }

}
