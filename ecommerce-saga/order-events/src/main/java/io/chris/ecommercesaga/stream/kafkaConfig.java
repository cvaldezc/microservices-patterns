package io.chris.ecommercesaga.stream;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;
import schema.model.event.OrderEvent;
import schema.model.event.PaymentEvent;

import java.util.Collections;

@Configuration
public class kafkaConfig {

    //@Bean
    public ReactiveKafkaProducerTemplate<String, OrderEvent> reactiveKafkaProducerTemplate(KafkaProperties kafkaProperties) {
        SenderOptions<String, OrderEvent> senderOptions = SenderOptions.create(kafkaProperties.buildProducerProperties());
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, PaymentEvent> reactiveKafkaConsumerTemplate(KafkaProperties kafkaProperties) {
        ReceiverOptions<String, PaymentEvent> receiverOptions = ReceiverOptions.<String, PaymentEvent>create(kafkaProperties.buildConsumerProperties())
                .subscription(Collections.singletonList("payment-service"));
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions);
    }
}
