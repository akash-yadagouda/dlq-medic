package dev.dlqmedic.mcp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Plan / apply for DLT replays. Staging is harmless (rows in our own tables); only executeReplay
 * produces to a live topic. All safety rules live here, in code, not in the model's instructions.
 */
@Service
public class ReplayService {

	/** The only topic a replay can ever write to. Not a parameter. */
	static final String REPLAY_TARGET_TOPIC = "orders";

	static final String PARK_TOPIC = "orders.parked";

	static final int CANARY_SIZE = 5;

	static final int MAX_STAGE_ITEMS = 500;

	/** Results that can report a server-side refusal, so the audit log can record REJECTED. */
	public interface Outcome {

		String status();

	}

	/** A vetted fix (see VettedFixes.CATALOGUE) to apply to a list of DLT messages. */
	public record Fix(String fix, List<String> messageIds) {
	}

	public record Rejection(String messageId, String reason) {
	}

	public record StageResult(String status, String batchId, int staged, List<String> skippedAlreadyProcessed,
			List<Rejection> rejected, List<String> samplePayloads, String nextStep) implements Outcome {
	}

	public record ExecuteResult(String status, String batchId, String phase, List<String> sent,
			List<String> skippedAlreadyProcessed, int remaining, String message) implements Outcome {
	}

	public record ParkResult(String status, int parked, List<Rejection> rejected) implements Outcome {
	}

	private final DltReader dltReader;

	private final OrderContract contract;

	private final VettedFixes fixes;

	private final JdbcClient jdbc;

	private final KafkaTemplate<String, String> kafka;

	public ReplayService(DltReader dltReader, OrderContract contract, VettedFixes fixes, JdbcClient jdbc,
			KafkaTemplate<String, String> kafka) {
		this.dltReader = dltReader;
		this.contract = contract;
		this.fixes = fixes;
		this.jdbc = jdbc;
		this.kafka = kafka;
	}

	public StageResult stage(String reason, List<Fix> requested, String utcOffset) {
		Map<String, String> items = new LinkedHashMap<>(); // messageId -> fix name
		List<Rejection> rejected = new ArrayList<>();
		for (Fix f : requested == null ? List.<Fix>of() : requested) {
			for (String messageId : f.messageIds() == null ? List.<String>of() : f.messageIds()) {
				if (items.putIfAbsent(messageId, f.fix()) != null) {
					rejected.add(new Rejection(messageId, "Listed under more than one fix"));
				}
			}
		}
		if (items.isEmpty() || items.size() > MAX_STAGE_ITEMS) {
			return new StageResult("REFUSED", null, 0, List.of(), rejected, List.of(),
					"Stage between 1 and " + MAX_STAGE_ITEMS + " messages per batch.");
		}
		Map<String, DltMessage> dlt = dltById();
		Set<String> alreadyInBatches = new HashSet<>(jdbc.sql("""
				SELECT i.message_id FROM dbo.replay_item i JOIN dbo.replay_batch b ON b.batch_id = i.batch_id
				WHERE i.status = 'SENT' OR b.status IN ('STAGED', 'CANARY_SENT')
				""").query(String.class).list());

		Map<String, String> accepted = new LinkedHashMap<>(); // messageId -> fixed payload json
		Set<String> seenOrderIds = new HashSet<>();
		items.forEach((messageId, fix) -> {
			DltMessage original = dlt.get(messageId);
			String problem = null;
			String payload = null;
			if (original == null) {
				problem = "No such message in " + DltReader.DLT_TOPIC;
			}
			else if (alreadyInBatches.contains(messageId)) {
				problem = "Message is already staged or replayed";
			}
			else {
				try {
					payload = fixes.apply(fix, original.value(), utcOffset);
				}
				catch (IllegalArgumentException ex) {
					problem = fix + ": " + ex.getMessage();
				}
			}
			if (problem == null && (problem = contract.violation(payload)) != null) {
				problem = "Still violates the contract after " + fix + ": " + problem;
			}
			if (problem == null && !original.key().equals(contract.orderId(payload))) {
				problem = "orderId must stay " + original.key();
			}
			if (problem == null && !seenOrderIds.add(original.key())) {
				problem = "Duplicate orderId in this batch";
			}
			if (problem != null) {
				rejected.add(new Rejection(messageId, problem));
			}
			else {
				accepted.put(messageId, payload);
			}
		});
		if (accepted.isEmpty()) {
			return new StageResult("REFUSED", null, 0, List.of(), rejected, List.of(), "Nothing valid to stage.");
		}

		Set<String> processed = existingOrderIds(accepted.keySet().stream().map(id -> dlt.get(id).key()).toList());
		if (processed.size() == accepted.size()) {
			return new StageResult("REFUSED", null, 0, List.copyOf(processed), rejected, List.of(),
					"All " + processed.size() + " orders are already processed; nothing to replay.");
		}
		String batchId = "b-" + UUID.randomUUID().toString().substring(0, 8);
		jdbc.sql("INSERT INTO dbo.replay_batch (batch_id, status, reason, item_count) VALUES (:id, 'STAGED', :reason, :n)")
			.param("id", batchId)
			.param("reason", reason == null ? "" : reason)
			.param("n", accepted.size())
			.update();
		List<String> skipped = new ArrayList<>();
		accepted.forEach((messageId, payload) -> {
			String orderId = dlt.get(messageId).key();
			boolean alreadyProcessed = processed.contains(orderId);
			if (alreadyProcessed) {
				skipped.add(orderId);
			}
			jdbc.sql("""
					INSERT INTO dbo.replay_item (batch_id, message_id, order_id, fixed_payload, status)
					VALUES (:batch, :msg, :order, :payload, :status)
					""")
				.param("batch", batchId)
				.param("msg", messageId)
				.param("order", orderId)
				.param("payload", payload)
				.param("status", alreadyProcessed ? "SKIPPED_ALREADY_PROCESSED" : "STAGED")
				.update();
		});
		int staged = accepted.size() - skipped.size();
		return new StageResult("STAGED", batchId, staged, skipped, rejected,
				accepted.values().stream().limit(2).toList(),
				"Call execute_replay(batchId=" + batchId + ", maxCount=" + CANARY_SIZE
						+ ") to send the canary. It requires human approval.");
	}

	public ExecuteResult execute(String batchId, int maxCount) throws Exception {
		String status = jdbc.sql("SELECT status FROM dbo.replay_batch WHERE batch_id = :id")
			.param("id", batchId)
			.query(String.class)
			.optional()
			.orElse(null);
		if (status == null || status.equals("COMPLETED")) {
			return refused(batchId, status == null ? "Unknown batch" : "Batch is already completed");
		}

		String phase;
		int limit;
		if (status.equals("STAGED")) {
			phase = "CANARY";
			limit = Math.min(Math.max(maxCount, 1), CANARY_SIZE);
		}
		else {
			List<String> canary = jdbc.sql("SELECT order_id FROM dbo.replay_item WHERE batch_id = :id AND status = 'SENT'")
				.param("id", batchId)
				.query(String.class)
				.list();
			Set<String> landed = existingOrderIds(canary);
			if (landed.size() < canary.size()) {
				return refused(batchId, "Canary not verified: " + (canary.size() - landed.size()) + " of " + canary.size()
						+ " canary orders are not in the orders table yet. Wait and check with find_existing_orders.");
			}
			phase = "BULK";
			limit = Math.max(maxCount, 1);
		}

		List<Map<String, Object>> pending = jdbc.sql("""
				SELECT TOP (:limit) message_id, order_id, fixed_payload FROM dbo.replay_item
				WHERE batch_id = :id AND status = 'STAGED' ORDER BY message_id
				""")
			.param("limit", limit)
			.param("id", batchId)
			.query()
			.listOfRows();
		// Re-check at send time: the world may have changed since staging (e.g. a hotfix re-send).
		Set<String> processedNow = existingOrderIds(pending.stream().map(r -> (String) r.get("order_id")).toList());

		List<String> sent = new ArrayList<>();
		List<String> skipped = new ArrayList<>();
		for (Map<String, Object> row : pending) {
			String messageId = (String) row.get("message_id");
			String orderId = (String) row.get("order_id");
			if (processedNow.contains(orderId)) {
				markItem(batchId, messageId, "SKIPPED_ALREADY_PROCESSED");
				skipped.add(orderId);
				continue;
			}
			ProducerRecord<String, String> out = new ProducerRecord<>(REPLAY_TARGET_TOPIC, orderId,
					(String) row.get("fixed_payload"));
			out.headers().add("producer", bytes("dlq-medic"));
			out.headers().add("x-replay-batch", bytes(batchId));
			out.headers().add("x-replayed-from", bytes(DltReader.DLT_TOPIC + "/" + messageId));
			kafka.send(out).get(10, TimeUnit.SECONDS);
			markItem(batchId, messageId, "SENT");
			sent.add(orderId);
		}

		int remaining = jdbc.sql("SELECT COUNT(*) FROM dbo.replay_item WHERE batch_id = :id AND status = 'STAGED'")
			.param("id", batchId)
			.query(Integer.class)
			.single();
		String newStatus = remaining == 0 ? "COMPLETED" : "CANARY_SENT";
		jdbc.sql("UPDATE dbo.replay_batch SET status = :s WHERE batch_id = :id")
			.param("s", newStatus)
			.param("id", batchId)
			.update();
		String message = phase.equals("CANARY")
				? "Canary sent. Verify these orders appear exactly once (find_existing_orders) before replaying the rest."
				: (remaining == 0 ? "Batch completed." : remaining + " items still staged.");
		return new ExecuteResult(newStatus, batchId, phase, sent, skipped, remaining, message);
	}

	public ParkResult park(List<String> messageIds, String reason) throws Exception {
		if (reason == null || reason.isBlank()) {
			return new ParkResult("REFUSED", 0, List.of(new Rejection("*", "A reason is required for the owning team.")));
		}
		Map<String, DltMessage> dlt = dltById();
		Set<String> inBatches = new HashSet<>(jdbc.sql("SELECT message_id FROM dbo.replay_item").query(String.class).list());
		List<Rejection> rejected = new ArrayList<>();
		int parked = 0;
		for (String messageId : messageIds) {
			DltMessage original = dlt.get(messageId);
			if (original == null) {
				rejected.add(new Rejection(messageId, "No such message in " + DltReader.DLT_TOPIC));
				continue;
			}
			if (inBatches.contains(messageId)) {
				rejected.add(new Rejection(messageId, "Message is part of a replay batch; not parking it"));
				continue;
			}
			ProducerRecord<String, String> out = new ProducerRecord<>(PARK_TOPIC, original.key(), original.value());
			out.headers().add("x-parked-reason", bytes(reason));
			out.headers().add("x-dlt-message-id", bytes(messageId));
			out.headers().add("x-dlt-error", bytes(String.valueOf(original.error())));
			kafka.send(out).get(10, TimeUnit.SECONDS);
			parked++;
		}
		return new ParkResult(parked > 0 ? "PARKED" : "REFUSED", parked, rejected);
	}

	public Set<String> existingOrderIds(List<String> orderIds) {
		if (orderIds.isEmpty()) {
			return Set.of();
		}
		return new HashSet<>(jdbc.sql("SELECT order_id FROM dbo.orders WHERE order_id IN (:ids)")
			.param("ids", orderIds)
			.query(String.class)
			.list());
	}

	private Map<String, DltMessage> dltById() {
		return dltReader.snapshot().stream().collect(Collectors.toMap(DltMessage::messageId, Function.identity()));
	}

	private void markItem(String batchId, String messageId, String status) {
		jdbc.sql("""
				UPDATE dbo.replay_item SET status = :s, sent_at = CASE WHEN :s = 'SENT' THEN SYSUTCDATETIME() END
				WHERE batch_id = :b AND message_id = :m
				""")
			.param("s", status)
			.param("b", batchId)
			.param("m", messageId)
			.update();
	}

	private static ExecuteResult refused(String batchId, String why) {
		return new ExecuteResult("REFUSED", batchId, null, List.of(), List.of(), -1, why);
	}

	private static byte[] bytes(String s) {
		return s.getBytes(StandardCharsets.UTF_8);
	}

}
