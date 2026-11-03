package io.chris.paymentservice.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;
import schema.model.event.OrderEvent;
import schema.model.event.PaymentEvent;

import java.math.BigDecimal;
import java.security.PrivateKey;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventAdapter {

    private final ReactiveKafkaConsumerTemplate<String, OrderEvent> kafkaConsumerTemplate;
    private final ReactiveKafkaProducerTemplate<String, PaymentEvent> kafkaProducerTemplate;

     @EventListener(ApplicationReadyEvent.class)
    public void handleOrderEvent() {
        log.info("🚀 Inicializando el flujo de escucha reactivo para order-events...");

        kafkaConsumerTemplate.receiveAutoAck()
                .doOnSubscribe(subscription -> log.info("🔗 Suscripción reactiva establecida internamente en la app"))
                .doOnNext(record -> log.info("¡Mensaje detectado en el Stream! Key: {}", record.key()))
                .flatMap(record -> {
                    try {
                        OrderEvent orderEvent = record.value();
                        PaymentEvent paymentEvent = validPayment(orderEvent);
                        return sendPaymentEvent(paymentEvent);
                    } catch (Exception e) {
                        log.error("Error al procesar deserialización de Jackson: ", e);
                        return Mono.empty();
                    }
                })
                .doOnError(throwable -> log.error("Error crítico en el stream", throwable))
                .retry()
                // CLAVE: Evitamos que el hilo se muera al arrancar la app y forzamos
                // a que Reactor Kafka tenga tiempo de negociar con el Broker.
                .subscribe();
    }

    private PaymentEvent validPayment(OrderEvent orderEvent) {
        var status = orderEvent.amount().compareTo(BigDecimal.valueOf(1000)) <= 0 ? "APPROVED" : "REJECTED";
        log.info("Received order status {}", status);
        return new PaymentEvent(orderEvent.orderId(), status);
    }

    private Mono<SenderResult<Void>> sendPaymentEvent(PaymentEvent paymentEvent) {
        return kafkaProducerTemplate.send("payment-service", String.valueOf(paymentEvent.orderId()),  paymentEvent)
                .doOnSuccess(result -> log.info("Mensaje enviado con éxito a la partición: {}", result.recordMetadata().partition()))
                .doOnError(err -> log.error("Error al enviar mensaje reactivo: {}", err.getMessage()));
    }

    /*@KafkaListener(topics = "order-events")
    public void listen(OrderEvent orderEvent) {
        log.info("Received order event {}", orderEvent);
    }*/
}
