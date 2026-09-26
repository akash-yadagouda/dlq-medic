package dev.dlqmedic.mcp;

import java.util.HashSet;
import java.util.Set;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Kafka keeps every DLT message forever, even after it has been dealt with. This tracks which
 * ones are already handled (staged, replayed, skipped or parked), so repeated or scheduled
 * runs only ever work on new failures.
 */
@Component
public class HandledMessages {

	private final JdbcClient jdbc;

	private final DltReader dltReader;

	public HandledMessages(JdbcClient jdbc, DltReader dltReader) {
		this.jdbc = jdbc;
		this.dltReader = dltReader;
	}

	/** DLT messageIds that belong to any replay batch (staged, sent or skipped). */
	public Set<String> inReplayBatches() {
		return new HashSet<>(jdbc.sql("SELECT DISTINCT message_id FROM dbo.replay_item").query(String.class).list());
	}

	public Set<String> parked() {
		return dltReader.parkedMessageIds();
	}

	public Set<String> all() {
		Set<String> handled = inReplayBatches();
		handled.addAll(parked());
		return handled;
	}

	/** Batches still waiting for a canary or bulk approval. */
	public int openBatches() {
		return jdbc.sql("SELECT COUNT(*) FROM dbo.replay_batch WHERE status IN ('STAGED', 'CANARY_SENT')")
			.query(Integer.class)
			.single();
	}

}
