package dev.dlqmedic.mcp;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * The only payload changes a replay may carry. The agent rehearses a fix in its sandbox and then
 * picks one of these by name; the server applies it. The model never writes the bytes that reach
 * the live topic, and each fix touches exactly one field, so identity can't drift.
 */
@Component
public class VettedFixes {

	static final String AMOUNT_STRING_TO_NUMBER = "amount_string_to_number";

	static final String EPOCH_MILLIS_TO_ISO8601 = "epoch_millis_to_iso8601";

	static final List<String> CATALOGUE = List.of(AMOUNT_STRING_TO_NUMBER, EPOCH_MILLIS_TO_ISO8601);

	private final ObjectMapper mapper;

	public VettedFixes(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	/** Applies a vetted fix to an original DLT payload. Throws IllegalArgumentException if it does not apply. */
	public String apply(String fix, String originalJson, String utcOffset) {
		ObjectNode payload = (ObjectNode) mapper.readTree(originalJson);
		switch (fix) {
			case AMOUNT_STRING_TO_NUMBER -> {
				JsonNode amount = payload.get("amount");
				if (amount == null || !amount.isString()) {
					throw new IllegalArgumentException("amount is not a string; this fix does not apply");
				}
				try {
					payload.put("amount", new BigDecimal(amount.asString().trim()));
				}
				catch (NumberFormatException ex) {
					throw new IllegalArgumentException("amount \"" + amount.asString() + "\" is not a decimal number");
				}
			}
			case EPOCH_MILLIS_TO_ISO8601 -> {
				JsonNode createdAt = payload.get("createdAt");
				if (createdAt == null || !createdAt.isIntegralNumber()) {
					throw new IllegalArgumentException("createdAt is not an epoch number; this fix does not apply");
				}
				ZoneOffset offset;
				try {
					offset = ZoneOffset.of(utcOffset == null ? "" : utcOffset);
				}
				catch (RuntimeException ex) {
					throw new IllegalArgumentException("utcOffset (e.g. \"+05:30\") is required for " + EPOCH_MILLIS_TO_ISO8601);
				}
				payload.put("createdAt", Instant.ofEpochMilli(createdAt.asLong())
					.atOffset(offset)
					.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			}
			default -> throw new IllegalArgumentException("Unknown fix '" + fix + "'. Vetted fixes: " + CATALOGUE);
		}
		return mapper.writeValueAsString(payload);
	}

}
