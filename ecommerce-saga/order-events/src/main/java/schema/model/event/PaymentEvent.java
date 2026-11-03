package schema.model.event;

public record PaymentEvent(Long orderId, String status) {
}
