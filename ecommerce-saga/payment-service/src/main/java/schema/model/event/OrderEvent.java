package schema.model.event;

import java.math.BigDecimal;

public record OrderEvent(Long orderId, String accountId, BigDecimal amount, String eventType) {
}
