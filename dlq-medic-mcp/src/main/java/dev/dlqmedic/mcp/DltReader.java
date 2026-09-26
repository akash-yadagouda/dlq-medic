package dev.dlqmedic.mcp;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * Reads a point-in-time snapshot of orders.DLT without joining a consumer group,
 * so looking at the DLT can never move anyone's committed offsets.
 */
@Component
public class DltReader {

	static final String DLT_TOPIC = "orders.DLT";

	private static final Duration READ_DEADLINE = Duration.ofSeconds(15);

	private final ConsumerFactory<String, String> consumerFactory;

	public DltReader(ConsumerFactory<String, String> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	/** All DLT records, ordered by partition then offset. */
	public List<DltMessage> snapshot() {
		Properties props = new Properties();
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
		try (Consumer<String, String> consumer = consumerFactory.createConsumer(null, "dlq-medic-reader", null, props)) {
			List<TopicPartition> partitions = consumer.partitionsFor(DLT_TOPIC)
				.stream()
				.map(info -> new TopicPartition(DLT_TOPIC, info.partition()))
				.toList();
			consumer.assign(partitions);
			consumer.seekToBeginning(partitions);
			Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

			List<DltMessage> messages = new ArrayList<>();
			Instant deadline = Instant.now().plus(READ_DEADLINE);
			while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
				if (Instant.now().isAfter(deadline)) {
					throw new IllegalStateException("Timed out reading " + DLT_TOPIC);
				}
				for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
					if (record.offset() < end.get(new TopicPartition(record.topic(), record.partition()))) {
						messages.add(DltMessage.from(record));
					}
				}
			}
			messages.sort(Comparator.comparing((DltMessage m) -> Integer.parseInt(m.messageId().split(":")[0]))
				.thenComparing(m -> Long.parseLong(m.messageId().split(":")[1])));
			return messages;
		}
	}

}
