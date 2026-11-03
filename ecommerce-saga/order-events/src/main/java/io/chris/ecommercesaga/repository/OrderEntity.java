package io.chris.ecommercesaga.repository;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;

@Data
@Table("orders")
public class OrderEntity {

    @Id
    private Long id;
    private String accountId;
    private BigDecimal amount;
    private String status;
}
