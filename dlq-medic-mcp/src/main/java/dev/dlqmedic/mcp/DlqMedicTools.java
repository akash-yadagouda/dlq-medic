package dev.dlqmedic.mcp;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * The agent's only way into Kafka and SQL Server. Read tools are annotated readOnly so TrueForge
 * never gates them; execute_replay is the single destructive tool and always pauses for a human.
 * Deliberately absent: delete topic, reset offsets, raw produce, arbitrary SQL.
 */
@Component
public class DlqMedicTools {

	public record DltPage(int total, int fromIndex, int returned, Integer nextIndex, List<DltMessage> messages) {
	}

	public record OrderCharges(String orderId, int charges) {
	}

	public record ExistingOrders(List<OrderCharges> existing, List<String> missing) {
	}

	private static final int MAX_PAGE = 50;

	private final PipelineHealth health;

	private final DltReader dltReader;

	private final HandledMessages handled;

	private final ReplayService replay;

	private final IncidentMemory memory;

	private final TeamNotifier notifier;

	private final AuditLog audit;

	private final JdbcClient jdbc;

	public DlqMedicTools(PipelineHealth health, DltReader dltReader, HandledMessages handled, ReplayService replay,
			IncidentMemory memory, TeamNotifier notifier, AuditLog audit, JdbcClient jdbc) {
		this.health = health;
		this.dltReader = dltReader;
		this.handled = handled;
		this.replay = replay;
		this.memory = memory;
		this.notifier = notifier;
		this.audit = audit;
		this.jdbc = jdbc;
	}

	@McpTool(name = "get_pipeline_health", description = """
			Snapshot of the order pipeline: message counts for orders / orders.DLT / orders.parked, \
			consumer lag of the order-service group, dltUnhandled (DLT messages not yet staged, replayed or \
			parked; the DLT itself keeps everything forever), openBatches (replay batches waiting for approval), \
			unhandled counts by producer-version and error class, and replay batch status. Start every investigation here.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false,
					idempotentHint = true, openWorldHint = false))
	public PipelineHealth.Snapshot getPipelineHealth() throws Exception {
		return audited("get_pipeline_health", null, health::snapshot);
	}

	@McpTool(name = "peek_dlt", description = """
			Reads dead-lettered orders from orders.DLT, ordered by partition then offset. Each message has \
			messageId ("partition:offset", used by all other tools), key (= orderId), the raw value, \
			errorClass, the one-line error, and producerVersion. Stack traces are omitted. By default only \
			unhandled messages are returned (not yet staged, replayed or parked). \
			Page with fromIndex/nextIndex; at most 50 per call.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false,
					idempotentHint = true, openWorldHint = false))
	public DltPage peekDlt(
			@McpToolParam(description = "0-based position to start from (use nextIndex from the previous page)", required = false) Integer fromIndex,
			@McpToolParam(description = "Messages to return, 1-50 (default 50)", required = false) Integer limit,
			@McpToolParam(description = "Only messages not yet staged, replayed or parked (default true)", required = false) Boolean unhandledOnly) throws Exception {
		int from = fromIndex == null ? 0 : Math.max(fromIndex, 0);
		int size = limit == null ? MAX_PAGE : Math.min(Math.max(limit, 1), MAX_PAGE);
		boolean onlyUnhandled = unhandledOnly == null || unhandledOnly;
		return audited("peek_dlt", Map.of("fromIndex", from, "limit", size, "unhandledOnly", onlyUnhandled), () -> {
			List<DltMessage> all = dltReader.snapshot();
			if (onlyUnhandled) {
				Set<String> done = handled.all();
				all = all.stream().filter(m -> !done.contains(m.messageId())).toList();
			}
			List<DltMessage> page = all.subList(Math.min(from, all.size()), Math.min(from + size, all.size()));
			Integer next = from + page.size() < all.size() ? from + page.size() : null;
			return new DltPage(all.size(), from, page.size(), next, page);
		});
	}

	@McpTool(name = "find_existing_orders", description = """
			Idempotency check against the orders database (read-only). For each orderId, reports whether it \
			is already processed and how many payment charges it has. Use it before staging a replay \
			(already-processed orders must not be replayed: the consumer is not idempotent and would charge \
			twice) and after a canary (each replayed order should now exist with exactly 1 charge).""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false,
					idempotentHint = true, openWorldHint = false))
	public ExistingOrders findExistingOrders(
			@McpToolParam(description = "Order ids to check (max 500)") List<String> orderIds) throws Exception {
		List<String> ids = orderIds.stream().distinct().limit(500).toList();
		return audited("find_existing_orders", Map.of("count", ids.size()), () -> {
			List<OrderCharges> existing = ids.isEmpty() ? List.of() : jdbc.sql("""
					SELECT o.order_id, (SELECT COUNT(*) FROM dbo.payment_ledger l WHERE l.order_id = o.order_id) AS charges
					FROM dbo.orders o WHERE o.order_id IN (:ids)
					""")
				.param("ids", ids)
				.query((rs, i) -> new OrderCharges(rs.getString("order_id"), rs.getInt("charges")))
				.list();
			List<String> found = existing.stream().map(OrderCharges::orderId).toList();
			return new ExistingOrders(existing, ids.stream().filter(id -> !found.contains(id)).toList());
		});
	}

	@McpTool(name = "stage_replay", description = """
			Stages a replay batch (plan step; sends nothing). You choose a vetted fix for each group of DLT \
			messageIds; the SERVER applies it to the original payload, re-validates the result against the orders \
			contract, and marks orders that are already processed as skipped. Vetted fixes: \
			amount_string_to_number (amount sent as a JSON string -> number), \
			epoch_millis_to_iso8601 (createdAt sent as epoch millis -> ISO-8601; needs utcOffset such as "+05:30"). \
			Messages no vetted fix can repair must be parked, not staged. \
			Returns batchId, staged count, skipped orderIds, per-message rejections and two sample payloads.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = false, destructiveHint = false,
					idempotentHint = false, openWorldHint = false))
	public ReplayService.StageResult stageReplay(
			@McpToolParam(description = "Why this batch is being replayed (root cause, fix applied)") String reason,
			@McpToolParam(description = "Groups of {fix: vetted fix name, messageIds: [\"partition:offset\", ...]}") List<ReplayService.Fix> fixes,
			@McpToolParam(description = "UTC offset for epoch_millis_to_iso8601, e.g. \"+05:30\" (take it from valid createdAt values)", required = false) String utcOffset) throws Exception {
		int count = fixes == null ? 0 : fixes.stream().mapToInt(f -> f.messageIds() == null ? 0 : f.messageIds().size()).sum();
		return audited("stage_replay", Map.of("reason", String.valueOf(reason), "messages", count,
				"utcOffset", String.valueOf(utcOffset)), () -> replay.stage(reason, fixes, utcOffset));
	}

	@McpTool(name = "execute_replay", description = """
			Replays a staged batch to the live `orders` topic (apply step). IRREVERSIBLE: the order consumer \
			will charge customers. Server-enforced rules: the first call on a batch sends at most 5 (canary); \
			further calls are refused until every canary order is visible in the orders table; already-processed \
			orders are skipped again at send time; the target topic is fixed. Requires human approval.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = false, destructiveHint = true,
					idempotentHint = false, openWorldHint = false))
	public ReplayService.ExecuteResult executeReplay(
			@McpToolParam(description = "Batch id returned by stage_replay") String batchId,
			@McpToolParam(description = "Maximum messages to send in this call") int maxCount) throws Exception {
		return audited("execute_replay", Map.of("batchId", batchId, "maxCount", maxCount),
				() -> replay.execute(batchId, maxCount));
	}

	@McpTool(name = "park_messages", description = """
			Copies DLT messages that cannot be fixed safely to orders.parked for the owning team, with the \
			reason attached. Additive and reversible: nothing is deleted, and the DLT keeps its copy.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = false, destructiveHint = false,
					idempotentHint = false, openWorldHint = false))
	public ReplayService.ParkResult parkMessages(
			@McpToolParam(description = "DLT messageIds (\"partition:offset\") to park") List<String> messageIds,
			@McpToolParam(description = "Why they cannot be replayed, for the owning team") String reason) throws Exception {
		return audited("park_messages", Map.of("count", messageIds.size(), "reason", String.valueOf(reason)),
				() -> replay.park(messageIds, reason));
	}

	@McpTool(name = "notify_owning_team", description = """
			Emails the team that owns the failing producer (IRREVERSIBLE: an email cannot be unsent; requires \
			human approval). Choose a team from the server's allowlist; you cannot address anyone else. Write a \
			short subject and a plain-text body: root cause, counts (replayed, skipped, parked), what the team must \
			do, and one recommendation. The server attaches parked-messages.csv built from orders.parked itself. \
			One email per replay batch.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = false, destructiveHint = true,
					idempotentHint = false, openWorldHint = true))
	public TeamNotifier.NotifyResult notifyOwningTeam(
			@McpToolParam(description = "Allow-listed team, e.g. checkout-team or oncall") String team,
			@McpToolParam(description = "Email subject (max 200 characters)") String subject,
			@McpToolParam(description = "Plain-text email body") String body,
			@McpToolParam(description = "Replay batch id of this incident", required = false) String batchId) throws Exception {
		return audited("notify_owning_team", Map.of("team", String.valueOf(team), "subject", String.valueOf(subject),
				"batchId", String.valueOf(batchId)), () -> notifier.notify(team, subject, body, batchId));
	}

	@McpTool(name = "recall_similar_incidents", description = """
			Long-term memory. Compares the error patterns of the current unhandled DLT messages (computed by the \
			server) with past incidents, and returns the most similar ones: similarity (0-1), shared patterns, \
			producer versions, root cause, server-verified facts (replayed, skipped, double charges), the human's \
			approve/deny decisions and lessons. Call it right after assessing. Memory is advice, not proof.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false,
					idempotentHint = true, openWorldHint = false))
	public IncidentMemory.Recall recallSimilarIncidents(
			@McpToolParam(description = "How many past incidents to return (default 3)", required = false) Integer limit) throws Exception {
		return audited("recall_similar_incidents", null, () -> memory.recall(limit == null ? 3 : limit));
	}

	@McpTool(name = "record_incident", description = """
			Saves this incident to long-term memory at the end of a run (append-only). Give the batchId, the \
			patterns (use the exact errorPattern values from peek_dlt, with count and the action taken), the root \
			cause, the human's approval/deny decisions with any reasons, and 1-2 lessons for next time. The server \
			computes the facts itself (replayed, skipped, double charges) from the batch and rejects unknown patterns.""",
			annotations = @McpTool.McpAnnotations(readOnlyHint = false, destructiveHint = false,
					idempotentHint = false, openWorldHint = false))
	public IncidentMemory.RecordResult recordIncident(
			@McpToolParam(description = "Replay batch id of this incident (omit if nothing was staged)", required = false) String batchId,
			@McpToolParam(description = "[{errorPattern, count, action}] with errorPattern exactly as returned by peek_dlt") List<IncidentMemory.PatternCount> patterns,
			@McpToolParam(description = "Root cause: producer version and what changed") String rootCause,
			@McpToolParam(description = "Each approval or denial by the human, with its reason", required = false) String humanDecisions,
			@McpToolParam(description = "1-2 lessons that would make the next similar incident faster or safer", required = false) String lessons) throws Exception {
		return audited("record_incident", Map.of("batchId", String.valueOf(batchId), "patterns", patterns == null ? 0 : patterns.size()),
				() -> memory.record(batchId, patterns, rootCause, humanDecisions, lessons));
	}

	@McpTool(name = "get_audit_log", description = "Most recent agent actions recorded by this server (newest first).",
			annotations = @McpTool.McpAnnotations(readOnlyHint = true, destructiveHint = false,
					idempotentHint = true, openWorldHint = false))
	public List<AuditLog.Entry> getAuditLog(
			@McpToolParam(description = "Entries to return, 1-50 (default 20)", required = false) Integer limit) {
		return audit.latest(limit == null ? 20 : Math.min(Math.max(limit, 1), 50));
	}

	private <T> T audited(String tool, Object args, Callable<T> action) throws Exception {
		try {
			T result = action.call();
			String outcome = result instanceof ReplayService.Outcome o && "REFUSED".equals(o.status()) ? "REJECTED" : "OK";
			audit.record(tool, args, outcome, summarize(result));
			return result;
		}
		catch (Exception ex) {
			audit.record(tool, args, "ERROR", ex.getMessage());
			throw ex;
		}
	}

	private static String summarize(Object result) {
		String text = String.valueOf(result);
		return text.length() > 1000 ? text.substring(0, 1000) + "…" : text;
	}

}
