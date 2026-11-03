package io.chris.ecommercesaga.stream;

import io.chris.ecommercesaga.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;
import schema.model.event.OrderEvent;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    // private final ReactiveKafkaProducerTemplate<String, OrderEvent> reactiveKafkaProducerTemplate;
    @Value("${app.order.topic}")
    private String topic;

    public Mono<Order> send(Order order) {
        /*return reactiveKafkaProducerTemplate.send(topic, String.format("Order-%d", order.id()), new OrderEvent(order.id(), order.accountId(), order.amount(), "CREATED"))
                .doOnSuccess(result -> log.info("Mensaje enviado, partition {}", result.recordMetadata().partition()))
                .doOnError(err -> log.error("Error al enviar mensajes"));*/
        return Mono.fromFuture(
                        kafkaTemplate.send(topic, String.format("Order-%d", order.id()), new OrderEvent(order.id(), order.accountId(), order.amount(), "CREATED"))
                )
                .doOnNext(result -> log.info("Metadata: {}", result.getRecordMetadata().toString()))
                .map(event -> event.getProducerRecord().value())
                .map(event -> new Order(event.orderId(), event.accountId(), event.amount()));
    }
}
