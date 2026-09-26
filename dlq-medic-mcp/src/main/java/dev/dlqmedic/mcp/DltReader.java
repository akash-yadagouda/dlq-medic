package dev.dlqmedic.mcp;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * Reads point-in-time snapshots of our topics without joining a consumer group,
 * so looking can never move anyone's committed offsets.
 */
@Component
public class DltReader {

	static final String DLT_TOPIC = "orders.DLT";

	static final String PARK_TOPIC = "orders.parked";

	/** Header park_messages writes on every parked copy, pointing back at the DLT message. */
	static final String PARKED_FROM_HEADER = "x-dlt-message-id";

	private static final Duration READ_DEADLINE = Duration.ofSeconds(15);

	private final ConsumerFactory<String, String> consumerFactory;

	public DltReader(ConsumerFactory<String, String> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	/** All DLT records, ordered by partition then offset. */
	public List<DltMessage> snapshot() {
		List<DltMessage> messages = new ArrayList<>(readTopic(DLT_TOPIC).stream().map(DltMessage::from).toList());
		messages.sort(Comparator.comparing((DltMessage m) -> Integer.parseInt(m.messageId().split(":")[0]))
			.thenComparing(m -> Long.parseLong(m.messageId().split(":")[1])));
		return messages;
	}

	/** One parked message, as park_messages wrote it to orders.parked. */
	public record Parked(String messageId, String orderId, String error, String producerVersion, String reason) {
	}

	/** Parked messages, one per DLT messageId (a repeated park never produces two rows). */
	public List<Parked> parkedMessages() {
		Map<String, Parked> byId = new LinkedHashMap<>();
		for (ConsumerRecord<String, String> r : readTopic(PARK_TOPIC)) {
			String messageId = header(r, PARKED_FROM_HEADER);
			if (messageId != null) {
				byId.putIfAbsent(messageId, new Parked(messageId, r.key(), header(r, "x-dlt-error"),
						header(r, "producer-version"), header(r, "x-parked-reason")));
			}
		}
		return List.copyOf(byId.values());
	}

	/** DLT messageIds that have already been parked. */
	public Set<String> parkedMessageIds() {
		return parkedMessages().stream().map(Parked::messageId).collect(Collectors.toSet());
	}

	private List<ConsumerRecord<String, String>> readTopic(String topic) {
		Properties props = new Properties();
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
		try (Consumer<String, String> consumer = consumerFactory.createConsumer(null, "dlq-medic-reader", null, props)) {
			List<TopicPartition> partitions = consumer.partitionsFor(topic)
				.stream()
				.map(info -> new TopicPartition(topic, info.partition()))
				.toList();
			consumer.assign(partitions);
			consumer.seekToBeginning(partitions);
			Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

			List<ConsumerRecord<String, String>> records = new ArrayList<>();
			Instant deadline = Instant.now().plus(READ_DEADLINE);
			while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
				if (Instant.now().isAfter(deadline)) {
					throw new IllegalStateException("Timed out reading " + topic);
				}
				for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
					if (record.offset() < end.get(new TopicPartition(record.topic(), record.partition()))) {
						records.add(record);
					}
				}
			}
			return records;
		}
	}

	private static String header(ConsumerRecord<?, ?> record, String name) {
		Header header = record.headers().lastHeader(name);
		return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
	}

}
