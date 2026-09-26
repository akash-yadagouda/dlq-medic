package dev.dlqmedic.mcp;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import jakarta.mail.internet.MimeMessage;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

/**
 * Emails the owning team after a human approves it. The model chooses a team from a server-side
 * allowlist (never a raw address) and writes the text; the server builds the attachment from
 * orders.parked itself and allows one email per replay batch.
 */
@Component
@EnableConfigurationProperties(TeamNotifier.NotifyProperties.class)
public class TeamNotifier {

	/** dlqmedic.notify.from and dlqmedic.notify.teams.<team>=<address> in application.properties. */
	@ConfigurationProperties("dlqmedic.notify")
	public record NotifyProperties(String from, Map<String, String> teams) {
	}

	public record NotifyResult(String status, String team, String recipient, String subject, int parkedRowsAttached,
			String message) implements ReplayService.Outcome {
	}

	private static final int MAX_SUBJECT = 200;

	private static final int MAX_BODY = 10_000;

	private final NotifyProperties props;

	private final JavaMailSender mail;

	private final DltReader dltReader;

	private final JdbcClient jdbc;

	public TeamNotifier(NotifyProperties props, JavaMailSender mail, DltReader dltReader, JdbcClient jdbc) {
		this.props = props;
		this.mail = mail;
		this.dltReader = dltReader;
		this.jdbc = jdbc;
	}

	public List<String> teams() {
		return List.copyOf(new TreeSet<>(props.teams().keySet()));
	}

	public NotifyResult notify(String team, String subject, String body, String batchId) throws Exception {
		String recipient = team == null ? null : props.teams().get(team);
		if (recipient == null) {
			return refused(team, subject, "Unknown team. Allowed teams: " + teams());
		}
		if (subject == null || subject.isBlank() || subject.length() > MAX_SUBJECT) {
			return refused(team, subject, "Subject is required (max " + MAX_SUBJECT + " characters).");
		}
		if (body == null || body.isBlank() || body.length() > MAX_BODY) {
			return refused(team, subject, "Body is required (max " + MAX_BODY + " characters).");
		}
		boolean hasBatch = batchId != null && !batchId.isBlank();
		if (hasBatch && jdbc.sql("SELECT COUNT(*) FROM dbo.notification_log WHERE batch_id = :b")
			.param("b", batchId)
			.query(Integer.class)
			.single() > 0) {
			return refused(team, subject, "The team was already emailed for batch " + batchId + ".");
		}

		List<DltReader.Parked> parked = dltReader.parkedMessages();
		MimeMessage message = mail.createMimeMessage();
		MimeMessageHelper email = new MimeMessageHelper(message, !parked.isEmpty(), StandardCharsets.UTF_8.name());
		email.setFrom(props.from());
		email.setTo(recipient);
		email.setSubject(subject);
		email.setText(body + "\n\n--\nSent by DLQ Medic after human approval." + (hasBatch ? " Replay batch " + batchId + "." : ""));
		if (!parked.isEmpty()) {
			email.addAttachment("parked-messages.csv", new ByteArrayResource(csv(parked).getBytes(StandardCharsets.UTF_8)), "text/csv");
		}
		mail.send(message);

		jdbc.sql("""
				INSERT INTO dbo.notification_log (batch_id, team, recipient, subject, parked_rows)
				VALUES (:batch, :team, :recipient, :subject, :rows)
				""")
			.param("batch", hasBatch ? batchId : null)
			.param("team", team)
			.param("recipient", recipient)
			.param("subject", subject)
			.param("rows", parked.size())
			.update();
		return new NotifyResult("SENT", team, recipient, subject, parked.size(),
				"Email sent" + (parked.isEmpty() ? "." : " with parked-messages.csv (" + parked.size() + " rows)."));
	}

	private static String csv(List<DltReader.Parked> parked) {
		StringBuilder out = new StringBuilder("messageId,orderId,error,producerVersion,reason\n");
		for (DltReader.Parked p : parked) {
			out.append(String.join(",", cell(p.messageId()), cell(p.orderId()), cell(p.error()), cell(p.producerVersion()),
					cell(p.reason())))
				.append('\n');
		}
		return out.toString();
	}

	private static String cell(String value) {
		return value == null ? "" : '"' + value.replace("\"", "\"\"") + '"';
	}

	private static NotifyResult refused(String team, String subject, String why) {
		return new NotifyResult("REFUSED", team, null, subject, 0, why);
	}

}
