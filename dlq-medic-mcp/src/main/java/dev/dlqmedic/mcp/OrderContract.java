package dev.dlqmedic.mcp;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.JsonNodeType;

/**
 * Server-side copy of the `orders` contract enforced by order-consumer. Every payload the agent
 * proposes is re-validated here, so a model that "thinks" it fixed a message cannot stage one
 * that would bounce straight back into the DLT.
 */
@Component
public class OrderContract {

	private final ObjectMapper mapper;

	public OrderContract(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	/** Returns null when valid, otherwise the first violation. */
	public String violation(String json) {
		JsonNode root;
		try {
			root = mapper.readTree(json);
		}
		catch (JacksonException ex) {
			return "Payload is not valid JSON";
		}
		String problem = requireString(root, "orderId");
		if (problem == null) problem = requireString(root, "customerId");
		if (problem == null) problem = requireString(root, "currency");
		if (problem == null) {
			JsonNode amount = root.get("amount");
			if (amount == null || !amount.isNumber()) {
				problem = "Field 'amount' must be a JSON number";
			}
			else if (amount.decimalValue().signum() <= 0) {
				problem = "Field 'amount' must be positive";
			}
		}
		if (problem == null) {
			JsonNode createdAt = root.get("createdAt");
			if (createdAt == null || createdAt.getNodeType() != JsonNodeType.STRING) {
				problem = "Field 'createdAt' must be an ISO-8601 timestamp string";
			}
			else {
				try {
					OffsetDateTime.parse(createdAt.asString());
				}
				catch (DateTimeParseException ex) {
					problem = "Field 'createdAt' must be an ISO-8601 timestamp string";
				}
			}
		}
		if (problem == null && root.get("currency").asString().length() != 3) {
			problem = "Field 'currency' must be an ISO-4217 code";
		}
		return problem;
	}

	public String orderId(String json) {
		JsonNode id = mapper.readTree(json).get("orderId");
		return id == null ? null : id.asString();
	}

	private static String requireString(JsonNode root, String field) {
		JsonNode node = root.get(field);
		if (node == null || node.isNull()) {
			return "Field '" + field + "' is required";
		}
		if (node.getNodeType() != JsonNodeType.STRING || node.asString().isBlank()) {
			return "Field '" + field + "' must be a non-blank string";
		}
		return null;
	}

}
