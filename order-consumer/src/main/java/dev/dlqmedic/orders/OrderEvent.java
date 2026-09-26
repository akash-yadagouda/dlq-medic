package dev.dlqmedic.orders;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/** Contract of the `orders` topic (v2 schema). */
public record OrderEvent(String orderId, String customerId, BigDecimal amount, String currency, OffsetDateTime createdAt) {
}
