package dev.dlqmedic.orders;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.JsonNodeType;

/**
 * Strict parser for the `orders` contract. Jackson's default coercion would quietly accept
 * "12.50" for a number; we reject type drift explicitly so bad producers are caught and the
 * error message says exactly which field broke.
 */
@Component
public class OrderEventParser {

	private final ObjectMapper mapper;

	public OrderEventParser(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	public OrderEvent parse(String json) {
		JsonNode root;
		try {
			root = mapper.readTree(json);
		}
		catch (JacksonException ex) {
			throw new InvalidOrderEventException("Payload is not valid JSON: " + ex.getOriginalMessage());
		}
		String orderId = requiredString(root, "orderId");
		String customerId = requiredString(root, "customerId");
		BigDecimal amount = requiredNumber(root, "amount");
		String currency = requiredString(root, "currency");
		OffsetDateTime createdAt = requiredIsoTimestamp(root, "createdAt");
		if (amount.signum() <= 0) {
			throw new InvalidOrderEventException("Field 'amount' must be positive but was " + amount);
		}
		if (currency.length() != 3) {
			throw new InvalidOrderEventException("Field 'currency' must be an ISO-4217 code but was \"" + currency + "\"");
		}
		return new OrderEvent(orderId, customerId, amount, currency, createdAt);
	}

	private static JsonNode required(JsonNode root, String field) {
		JsonNode node = root.get(field);
		if (node == null || node.isNull()) {
			throw new InvalidOrderEventException("Field '" + field + "' is required but was missing");
		}
		return node;
	}

	private static String requiredString(JsonNode root, String field) {
		JsonNode node = required(root, field);
		if (node.getNodeType() != JsonNodeType.STRING || node.asString().isBlank()) {
			throw typeMismatch(field, "a non-blank string", node);
		}
		return node.asString();
	}

	private static BigDecimal requiredNumber(JsonNode root, String field) {
		JsonNode node = required(root, field);
		if (!node.isNumber()) {
			throw typeMismatch(field, "a JSON number", node);
		}
		return node.decimalValue();
	}

	private static OffsetDateTime requiredIsoTimestamp(JsonNode root, String field) {
		JsonNode node = required(root, field);
		if (node.getNodeType() != JsonNodeType.STRING) {
			throw typeMismatch(field, "an ISO-8601 timestamp string", node);
		}
		try {
			return OffsetDateTime.parse(node.asString());
		}
		catch (DateTimeParseException ex) {
			throw typeMismatch(field, "an ISO-8601 timestamp string", node);
		}
	}

	private static InvalidOrderEventException typeMismatch(String field, String expected, JsonNode actual) {
		return new InvalidOrderEventException("Field '" + field + "' must be " + expected + " but was "
				+ actual.getNodeType() + " (" + actual + ")");
	}

}
