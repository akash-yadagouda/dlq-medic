package dev.dlqmedic.orders;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {

	private final OrderEventParser parser;

	private final OrderService service;

	public OrderListener(OrderEventParser parser, OrderService service) {
		this.parser = parser;
		this.service = service;
	}

	@KafkaListener(topics = "orders", groupId = "order-service")
	public void onOrder(ConsumerRecord<String, String> record) {
		service.process(parser.parse(record.value()), record);
	}

}
