package dev.dlqmedic.mcp;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
public class PipelineHealth {

	public record BatchSummary(String batchId, String status, int items, int sent, int staged, int skipped) {
	}

	/**
	 * dltUnhandled counts DLT messages not yet staged, replayed, skipped or parked; the breakdowns cover only
	 * those. openBatches counts replay batches still waiting for a canary or bulk approval.
	 */
	public record Snapshot(Map<String, Long> topicMessageCounts, String consumerGroup, long consumerLag,
			long dltUnhandled, long dltParked, int openBatches, Map<String, Long> unhandledByProducerVersion,
			Map<String, Long> unhandledByErrorClass, List<BatchSummary> replayBatches) {
	}

	private static final List<String> TOPICS = List.of("orders", DltReader.DLT_TOPIC, DltReader.PARK_TOPIC);

	private static final String CONSUMER_GROUP = "order-service";

	private final KafkaAdmin kafkaAdmin;

	private final DltReader dltReader;

	private final HandledMessages handled;

	private final JdbcClient jdbc;

	public PipelineHealth(KafkaAdmin kafkaAdmin, DltReader dltReader, HandledMessages handled, JdbcClient jdbc) {
		this.kafkaAdmin = kafkaAdmin;
		this.dltReader = dltReader;
		this.handled = handled;
		this.jdbc = jdbc;
	}

	public Snapshot snapshot() throws Exception {
		try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
			List<TopicPartition> partitions = admin.describeTopics(TOPICS)
				.allTopicNames()
				.get()
				.values()
				.stream()
				.flatMap(d -> d.partitions().stream().map(p -> new TopicPartition(d.name(), p.partition())))
				.toList();
			Map<TopicPartition, ListOffsetsResultInfo> earliest = admin
				.listOffsets(partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.earliest())))
				.all()
				.get();
			Map<TopicPartition, ListOffsetsResultInfo> latest = admin
				.listOffsets(partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())))
				.all()
				.get();

			Map<String, Long> counts = new LinkedHashMap<>();
			TOPICS.forEach(t -> counts.put(t, 0L));
			for (TopicPartition tp : partitions) {
				counts.merge(tp.topic(), latest.get(tp).offset() - earliest.get(tp).offset(), Long::sum);
			}

			Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
				.partitionsToOffsetAndMetadata()
				.get();
			long lag = partitions.stream()
				.filter(tp -> tp.topic().equals("orders"))
				.mapToLong(tp -> {
					OffsetAndMetadata c = committed.get(tp);
					return latest.get(tp).offset() - (c == null ? earliest.get(tp).offset() : c.offset());
				})
				.sum();

			Set<String> parked = handled.parked();
			Set<String> done = handled.inReplayBatches();
			done.addAll(parked);
			List<DltMessage> unhandled = dltReader.snapshot().stream().filter(m -> !done.contains(m.messageId())).toList();
			return new Snapshot(counts, CONSUMER_GROUP, lag, unhandled.size(), parked.size(), handled.openBatches(),
					countBy(unhandled, DltMessage::producerVersion), countBy(unhandled, DltMessage::errorClass), batches());
		}
	}

	private static Map<String, Long> countBy(List<DltMessage> dlt, Function<DltMessage, String> key) {
		return dlt.stream()
			.collect(Collectors.groupingBy(m -> String.valueOf(key.apply(m)), TreeMap::new, Collectors.counting()));
	}

	private List<BatchSummary> batches() {
		return jdbc.sql("""
				SELECT b.batch_id, b.status, b.item_count,
				       SUM(CASE WHEN i.status = 'SENT' THEN 1 ELSE 0 END) AS sent,
				       SUM(CASE WHEN i.status = 'STAGED' THEN 1 ELSE 0 END) AS staged,
				       SUM(CASE WHEN i.status = 'SKIPPED_ALREADY_PROCESSED' THEN 1 ELSE 0 END) AS skipped
				FROM dbo.replay_batch b LEFT JOIN dbo.replay_item i ON i.batch_id = b.batch_id
				GROUP BY b.batch_id, b.status, b.item_count, b.created_at
				ORDER BY b.created_at
				""")
			.query((rs, i) -> new BatchSummary(rs.getString("batch_id"), rs.getString("status"), rs.getInt("item_count"),
					rs.getInt("sent"), rs.getInt("staged"), rs.getInt("skipped")))
			.list();
	}

}
