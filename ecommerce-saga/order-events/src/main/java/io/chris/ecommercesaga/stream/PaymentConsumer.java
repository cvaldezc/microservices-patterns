package io.chris.ecommercesaga.stream;

import io.chris.ecommercesaga.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;
import schema.model.event.PaymentEvent;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentConsumer {

    private final OrderRepository orderRepository;
    private final ReactiveKafkaConsumerTemplate<String, PaymentEvent> kafkaConsumerTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void consume() {
        kafkaConsumerTemplate.receiveAutoAck()
                .map(ConsumerRecord::value)
                .flatMap(payment ->
                        orderRepository.findById(payment.orderId())
                            .flatMap(entity -> {
                                var status = payment.status().equals("APPROVED") ? "CONFIRMED" : "CANCELLED" ;
                                entity.setStatus(status);
                                log.info("Status received: {}", status);
                                return orderRepository.save(entity);
                            })
                )
                .doOnError(throwable -> log.error("Error while consuming payment", throwable))
                .retry()
                .subscribe();

    }
}
