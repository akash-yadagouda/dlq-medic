package dev.dlqmedic.mcp;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

/** Append-only trail of every tool call. The dlq_medic DB login is DENY UPDATE/DELETE on this table. */
@Component
public class AuditLog {

	public record Entry(long auditId, String at, String tool, String args, String outcome, String detail) {
	}

	private final JdbcClient jdbc;

	private final ObjectMapper mapper;

	public AuditLog(JdbcClient jdbc, ObjectMapper mapper) {
		this.jdbc = jdbc;
		this.mapper = mapper;
	}

	public void record(String tool, Object args, String outcome, String detail) {
		jdbc.sql("INSERT INTO dbo.agent_audit_log (tool, args, outcome, detail) VALUES (:tool, :args, :outcome, :detail)")
			.param("tool", tool)
			.param("args", args == null ? null : mapper.writeValueAsString(args))
			.param("outcome", outcome)
			.param("detail", detail)
			.update();
	}

	public List<Entry> latest(int limit) {
		return jdbc.sql("""
				SELECT TOP (:limit) audit_id, CONVERT(VARCHAR(30), at, 126) AS at, tool, args, outcome, detail
				FROM dbo.agent_audit_log ORDER BY audit_id DESC
				""")
			.param("limit", limit)
			.query((rs, i) -> new Entry(rs.getLong("audit_id"), rs.getString("at"), rs.getString("tool"),
					rs.getString("args"), rs.getString("outcome"), rs.getString("detail")))
			.list();
	}

}
