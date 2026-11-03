package io.chris.ecommercesaga.expose.web;

import io.chris.ecommercesaga.model.Order;
import io.chris.ecommercesaga.service.OrderUseCase;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderUseCase orderUseCase;

    @PostMapping
    public Mono<Order> saveOrder(@RequestBody Order order) {
        return orderUseCase.createOrder(order);
    }

    @GetMapping("/{id}")
    public Mono<Order> getOrders(@PathVariable Long id) {
        return orderUseCase.getOrder(id);
    }
}
