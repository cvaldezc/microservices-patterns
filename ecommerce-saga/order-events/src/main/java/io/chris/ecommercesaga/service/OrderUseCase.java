package io.chris.ecommercesaga.service;

import io.chris.ecommercesaga.model.Order;
import reactor.core.publisher.Mono;

public interface OrderUseCase {

    Mono<Order> createOrder(Order order);

    Mono<Order> getOrder(Long id);
}
