package com.ecommerce.order.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.OrderCreatedEvent;
import com.ecommerce.order.entity.OutboxEventEntity;
import com.ecommerce.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final int MAX_RETRIES = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Periodically scans outbox_events table every 1 second to publish OrderCreatedEvent to Kafka topic order-created-events.
     * Awaits synchronous ACK before updating status to SENT.
     */
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingOutboxEvents() {
        List<OutboxEventEntity> pendingEvents =
                outboxEventRepository.findTop20ByStatusOrderByCreatedAtAsc("PENDING");

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("🚀 [OutboxPublisher] Found {} PENDING outbox events awaiting publication to Kafka...", pendingEvents.size());

        for (OutboxEventEntity outbox : pendingEvents) {
            try {
                OrderCreatedEvent dto = objectMapper.readValue(outbox.getPayload(), OrderCreatedEvent.class);
                String partitionKey = dto.getCustomerId();

                ProducerRecord<String, String> record = new ProducerRecord<>(
                        KafkaTopicConstants.TOPIC_ORDER_CREATED,
                        partitionKey,
                        outbox.getPayload()
                );

                if (outbox.getTraceparent() != null && !outbox.getTraceparent().isBlank()) {
                    record.headers().add("traceparent", outbox.getTraceparent().getBytes(StandardCharsets.UTF_8));
                }

                SendResult<String, String> result = kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

                outbox.setStatus("SENT");
                outbox.setSentAt(LocalDateTime.now());
                outbox.setLastError(null);
                outboxEventRepository.save(outbox);

                log.info("✅ [Outbox -> Kafka] Successfully published event {} (Order: {}) to Partition: {}, Offset: {}",
                        outbox.getEventId(),
                        dto.getOrderNumber(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());

            } catch (Exception e) {
                outbox.incrementRetryCount();
                String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                outbox.setLastError(errorMsg);

                if (outbox.getRetryCount() >= MAX_RETRIES) {
                    outbox.setStatus("FAILED_MAX_RETRIES");
                    log.error("💥 [OutboxPublisher] Event {} reached max retries ({}/{}). Marked as FAILED_MAX_RETRIES!",
                            outbox.getEventId(), outbox.getRetryCount(), MAX_RETRIES);
                } else {
                    log.warn("⚠️ [OutboxPublisher] Kafka send failed (attempt {}/{}) for event {}: {}",
                            outbox.getRetryCount(), MAX_RETRIES, outbox.getEventId(), errorMsg);
                }
                outboxEventRepository.save(outbox);
            }
        }
    }
}
