package dev.dlqmedic.mcp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/**
 * One dead-lettered record, reduced to what an operator (or the agent) needs. The stack trace header
 * is deliberately dropped: it is ~1,200 tokens per message and adds nothing the cause message lacks.
 */
public record DltMessage(String messageId, String key, String value, String errorClass, String error,
		String errorPattern, String producerVersion, String originalTopic, Integer originalPartition,
		Long originalOffset, Long originalTimestamp) {

	private static final String WRAPPER_MARKER = "threw exception; ";

	/** A trailing "(<offending value>)" in the error message; dropped to get a stable pattern key. */
	private static final Pattern OFFENDING_VALUE = Pattern.compile("\\s*\\(.*\\)\\s*$");

	/** Stable id used by every tool: "<DLT partition>:<DLT offset>". */
	static String idOf(int partition, long offset) {
		return partition + ":" + offset;
	}

	static DltMessage from(ConsumerRecord<String, String> record) {
		String causeClass = text(record, "kafka_dlt-exception-cause-fqcn");
		if (causeClass == null) {
			causeClass = text(record, "kafka_dlt-exception-fqcn");
		}
		String message = text(record, "kafka_dlt-exception-message");
		if (message != null && message.contains(WRAPPER_MARKER)) {
			message = message.substring(message.indexOf(WRAPPER_MARKER) + WRAPPER_MARKER.length());
		}
		return new DltMessage(idOf(record.partition(), record.offset()), record.key(), record.value(),
				causeClass == null ? null : causeClass.substring(causeClass.lastIndexOf('.') + 1), message,
				patternOf(message), text(record, "producer-version"), text(record, "kafka_dlt-original-topic"),
				intHeader(record, "kafka_dlt-original-partition"), longHeader(record, "kafka_dlt-original-offset"),
				longHeader(record, "kafka_dlt-original-timestamp"));
	}

	/**
	 * Canonical error pattern, e.g. "Field 'amount' must be a JSON number but was STRING". Computed by the
	 * server so the same failure always gets the same key, however the model would phrase it.
	 */
	static String patternOf(String error) {
		return error == null ? "unknown" : OFFENDING_VALUE.matcher(error).replaceFirst("");
	}

	private static byte[] raw(ConsumerRecord<?, ?> record, String name) {
		Header header = record.headers().lastHeader(name);
		return header == null ? null : header.value();
	}

	private static String text(ConsumerRecord<?, ?> record, String name) {
		byte[] bytes = raw(record, name);
		return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
	}

	// Spring writes the original partition/offset/timestamp headers as big-endian binary, not text.
	private static Integer intHeader(ConsumerRecord<?, ?> record, String name) {
		byte[] bytes = raw(record, name);
		return bytes == null || bytes.length != Integer.BYTES ? null : ByteBuffer.wrap(bytes).getInt();
	}

	private static Long longHeader(ConsumerRecord<?, ?> record, String name) {
		byte[] bytes = raw(record, name);
		return bytes == null || bytes.length != Long.BYTES ? null : ByteBuffer.wrap(bytes).getLong();
	}

}
