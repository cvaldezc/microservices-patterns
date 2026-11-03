package io.chris.paymentservice.config;

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
public class ReactiveKafkaConfig {

    @Bean
    public ReactiveKafkaConsumerTemplate<String, OrderEvent> kafkaConsumerTemplate(KafkaProperties kafkaProperties) {
        ReceiverOptions<String, OrderEvent> receiverOptions = ReceiverOptions.<String, OrderEvent>create(kafkaProperties.buildConsumerProperties(null))
                .subscription(Collections.singletonList("order-events"));
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions);
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, PaymentEvent> kafkaProducerTemplate(KafkaProperties kafkaProperties) {
        SenderOptions<String, PaymentEvent> senderOptions = SenderOptions.<String, PaymentEvent>create(kafkaProperties.buildProducerProperties(null));
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }
}

