package dev.dlqmedic.mcp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * Long-term memory across incidents. Matching uses server-computed error patterns, and the recorded
 * outcome (replayed, skipped, double charges) is computed from the batch by the server: the model
 * only contributes interpretation (root cause, human decisions, lessons), never the facts.
 */
@Component
public class IncidentMemory {

	public record PatternCount(String errorPattern, int count, String action) {
	}

	public record Facts(int replayed, int skippedAlreadyProcessed, int doubleCharges) {
	}

	public record Incident(long incidentId, String recordedAt, String batchId, List<String> producerVersions,
			List<PatternCount> patterns, String rootCause, Facts facts, String humanDecisions, String lessons) {
	}

	public record Match(double similarity, List<String> sharedPatterns, boolean sameProducerVersion, Incident incident) {
	}

	public record Recall(List<String> currentPatterns, List<String> currentProducerVersions, List<Match> matches,
			String note) {
	}

	public record RecordResult(String status, Long incidentId, Facts facts, List<String> rejectedPatterns,
			String message) implements ReplayService.Outcome {
	}

	private static final int HISTORY_WINDOW = 50;

	private final JdbcClient jdbc;

	private final DltReader dltReader;

	private final HandledMessages handled;

	private final ObjectMapper mapper;

	public IncidentMemory(JdbcClient jdbc, DltReader dltReader, HandledMessages handled, ObjectMapper mapper) {
		this.jdbc = jdbc;
		this.dltReader = dltReader;
		this.handled = handled;
		this.mapper = mapper;
	}

	/** Compares today's unhandled DLT failures with past incidents (Jaccard similarity of error patterns). */
	public Recall recall(int limit) {
		Set<String> done = handled.all();
		List<DltMessage> unhandled = dltReader.snapshot().stream().filter(m -> !done.contains(m.messageId())).toList();
		Set<String> patterns = unhandled.stream().map(DltMessage::errorPattern).collect(Collectors.toCollection(TreeSet::new));
		Set<String> versions = unhandled.stream().map(DltMessage::producerVersion).collect(Collectors.toCollection(TreeSet::new));
		if (patterns.isEmpty()) {
			return new Recall(List.of(), List.of(), List.of(), "No unhandled DLT messages, so there is nothing to match.");
		}
		List<Match> matches = new ArrayList<>();
		for (Incident past : history()) {
			Set<String> pastPatterns = past.patterns().stream().map(PatternCount::errorPattern).collect(Collectors.toSet());
			Set<String> shared = new TreeSet<>(patterns);
			shared.retainAll(pastPatterns);
			if (shared.isEmpty()) {
				continue;
			}
			Set<String> union = new HashSet<>(patterns);
			union.addAll(pastPatterns);
			double similarity = Math.round(100.0 * shared.size() / union.size()) / 100.0;
			boolean sameVersion = past.producerVersions().stream().anyMatch(versions::contains);
			matches.add(new Match(similarity, List.copyOf(shared), sameVersion, past));
		}
		matches.sort(Comparator.comparingDouble(Match::similarity).reversed()
			.thenComparing(m -> m.incident().incidentId(), Comparator.reverseOrder()));
		List<Match> top = matches.stream().limit(Math.max(limit, 1)).toList();
		return new Recall(List.copyOf(patterns), List.copyOf(versions), top,
				top.isEmpty() ? "No similar past incident: this failure is new." : "Memory is advice, not proof: validate as usual.");
	}

	public RecordResult record(String batchId, List<PatternCount> patterns, String rootCause, String humanDecisions,
			String lessons) {
		if (rootCause == null || rootCause.isBlank() || patterns == null || patterns.isEmpty()) {
			return new RecordResult("REFUSED", null, null, List.of(), "rootCause and at least one pattern are required.");
		}
		List<DltMessage> dlt = dltReader.snapshot();
		Set<String> known = dlt.stream().map(DltMessage::errorPattern).collect(Collectors.toSet());
		List<String> rejected = patterns.stream().map(PatternCount::errorPattern).filter(p -> !known.contains(p)).toList();
		List<PatternCount> accepted = patterns.stream().filter(p -> known.contains(p.errorPattern())).toList();
		if (accepted.isEmpty()) {
			return new RecordResult("REFUSED", null, null, rejected,
					"None of the patterns exist in orders.DLT. Use the errorPattern values returned by peek_dlt.");
		}
		Facts facts = new Facts(0, 0, 0);
		if (batchId != null && !batchId.isBlank()) {
			if (jdbc.sql("SELECT COUNT(*) FROM dbo.replay_batch WHERE batch_id = :b").param("b", batchId).query(Integer.class).single() == 0) {
				return new RecordResult("REFUSED", null, null, rejected, "Unknown batch " + batchId);
			}
			if (jdbc.sql("SELECT COUNT(*) FROM dbo.incident_memory WHERE batch_id = :b").param("b", batchId).query(Integer.class).single() > 0) {
				return new RecordResult("REFUSED", null, null, rejected, "Batch " + batchId + " is already recorded.");
			}
			facts = factsOf(batchId);
		}
		Set<String> acceptedPatterns = accepted.stream().map(PatternCount::errorPattern).collect(Collectors.toSet());
		Set<String> versions = dlt.stream()
			.filter(m -> acceptedPatterns.contains(m.errorPattern()))
			.map(DltMessage::producerVersion)
			.collect(Collectors.toCollection(TreeSet::new));
		long id = jdbc.sql("""
				INSERT INTO dbo.incident_memory (batch_id, producer_versions, patterns, root_cause, outcome, human_decisions, lessons)
				OUTPUT INSERTED.incident_id
				VALUES (:batch, :versions, :patterns, :rootCause, :outcome, :decisions, :lessons)
				""")
			.param("batch", batchId == null || batchId.isBlank() ? null : batchId)
			.param("versions", String.join(",", versions))
			.param("patterns", mapper.writeValueAsString(accepted))
			.param("rootCause", rootCause)
			.param("outcome", mapper.writeValueAsString(facts))
			.param("decisions", humanDecisions)
			.param("lessons", lessons)
			.query(Long.class)
			.single();
		return new RecordResult("RECORDED", id, facts, rejected,
				"Remembered. Future incidents with these patterns will recall it.");
	}

	private Facts factsOf(String batchId) {
		int replayed = count("SELECT COUNT(*) FROM dbo.replay_item WHERE batch_id = :b AND status = 'SENT'", batchId);
		int skipped = count("SELECT COUNT(*) FROM dbo.replay_item WHERE batch_id = :b AND status = 'SKIPPED_ALREADY_PROCESSED'", batchId);
		int doubleCharges = count("""
				SELECT COUNT(*) FROM (
				    SELECT l.order_id FROM dbo.payment_ledger l
				    WHERE l.order_id IN (SELECT order_id FROM dbo.replay_item WHERE batch_id = :b)
				    GROUP BY l.order_id HAVING COUNT(*) > 1) d
				""", batchId);
		return new Facts(replayed, skipped, doubleCharges);
	}

	private int count(String sql, String batchId) {
		return jdbc.sql(sql).param("b", batchId).query(Integer.class).single();
	}

	private List<Incident> history() {
		return jdbc.sql("""
				SELECT TOP (:n) incident_id, CONVERT(VARCHAR(30), recorded_at, 126) AS recorded_at, batch_id,
				       producer_versions, patterns, root_cause, outcome, human_decisions, lessons
				FROM dbo.incident_memory ORDER BY incident_id DESC
				""")
			.param("n", HISTORY_WINDOW)
			.query((rs, i) -> new Incident(rs.getLong("incident_id"), rs.getString("recorded_at"), rs.getString("batch_id"),
					List.of(rs.getString("producer_versions").split(",")),
					mapper.readValue(rs.getString("patterns"), new TypeReference<List<PatternCount>>() {
					}),
					rs.getString("root_cause"), mapper.readValue(rs.getString("outcome"), Facts.class),
					rs.getString("human_decisions"), rs.getString("lessons")))
			.list();
	}

}
