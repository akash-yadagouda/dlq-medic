package dev.dlqmedic.orders;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Transient failures (e.g. DB blips) are retried twice; contract violations are not retried at all.
 * Anything that still fails is published to `orders.DLT` (same partition) with Spring's
 * kafka_dlt-* headers: exception class, message, original topic/partition/offset.
 */
@Configuration
public class KafkaErrorHandlingConfig {

	private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

	@Bean
	DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
		// Explicit destination: Spring Kafka 4 changed the default suffix to "-dlt", and with
		// auto-create off a wrong name blocks every send for max.block.ms.
		DeadLetterPublishingRecoverer toDlt = new DeadLetterPublishingRecoverer(template,
				(record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
		DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
			Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
			log.warn("→ orders.DLT  {}-{}@{} key={} : {}", record.topic(), record.partition(), record.offset(),
					record.key(), cause.getMessage());
			toDlt.accept(record, ex);
		}, new FixedBackOff(500L, 2));
		handler.addNotRetryableExceptions(InvalidOrderEventException.class);
		handler.setLogLevel(KafkaException.Level.DEBUG);
		return handler;
	}

}
