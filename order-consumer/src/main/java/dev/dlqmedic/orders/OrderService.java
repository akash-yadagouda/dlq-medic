package dev.dlqmedic.orders;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Upserts the order and appends a charge. Like many real consumers, it is NOT idempotent:
 * processing the same order twice charges the customer twice. That is why a blind DLT replay is dangerous.
 */
@Service
public class OrderService {

	private final JdbcClient jdbc;

	public OrderService(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	@Transactional
	public void process(OrderEvent event, ConsumerRecord<String, String> source) {
		jdbc.sql("""
				MERGE dbo.orders WITH (HOLDLOCK) AS t
				USING (SELECT :orderId AS order_id) AS s ON t.order_id = s.order_id
				WHEN MATCHED THEN UPDATE SET
				    customer_id = :customerId, amount = :amount, currency = :currency, created_at = :createdAt,
				    source_topic = :topic, source_partition = :partition, source_offset = :offset,
				    processed_at = SYSUTCDATETIME()
				WHEN NOT MATCHED THEN INSERT
				    (order_id, customer_id, amount, currency, created_at, source_topic, source_partition, source_offset)
				    VALUES (:orderId, :customerId, :amount, :currency, :createdAt, :topic, :partition, :offset);
				""")
			.param("orderId", event.orderId())
			.param("customerId", event.customerId())
			.param("amount", event.amount())
			.param("currency", event.currency())
			.param("createdAt", event.createdAt())
			.param("topic", source.topic())
			.param("partition", source.partition())
			.param("offset", source.offset())
			.update();
		jdbc.sql("INSERT INTO dbo.payment_ledger (order_id, amount) VALUES (:orderId, :amount)")
			.param("orderId", event.orderId())
			.param("amount", event.amount())
			.update();
	}

}
