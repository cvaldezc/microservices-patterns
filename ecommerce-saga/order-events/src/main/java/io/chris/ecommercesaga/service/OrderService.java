package io.chris.ecommercesaga.service;

import io.chris.ecommercesaga.model.Order;
import io.chris.ecommercesaga.repository.OrderEntity;
import io.chris.ecommercesaga.repository.OrderRepository;
import io.chris.ecommercesaga.stream.OrderProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService implements OrderUseCase {

    private final OrderRepository orderRepository;
    private final OrderProducer orderProducer;

    @Override
    public Mono<Order> createOrder(Order order) {
        var unsaved = new OrderEntity();
        unsaved.setAccountId(order.accountId());
        unsaved.setAmount(order.amount());
        unsaved.setStatus("PENDING");
        return orderRepository.save(unsaved)
                .map(entity -> new Order(entity.getId(), entity.getAccountId(), entity.getAmount(), entity.getStatus()))
                .flatMap(orderProducer::send);
    }

    @Override
    public Mono<Order> getOrder(Long id) {
        return orderRepository.findById(id)
                .map(entity -> new Order(entity.getId(), entity.getAccountId(), entity.getAmount(), entity.getStatus()));
    }
}

