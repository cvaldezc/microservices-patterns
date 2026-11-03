package io.chris.ecommercesaga.model;

import java.math.BigDecimal;

public record Order(Long id, String accountId, BigDecimal amount, String status) {

    public Order (Long id, String accountId, BigDecimal amount) {
        this(id, accountId, amount, null);
    }
}
