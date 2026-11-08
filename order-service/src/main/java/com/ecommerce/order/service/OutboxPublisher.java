package com.ecommerce.order.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.OrderCreatedEvent;
import com.ecommerce.order.entity.OutboxEventEntity;
import com.ecommerce.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int MAX_RETRIES = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Quét định kỳ bảng outbox_events mỗi 1 giây để đẩy OrderCreatedEvent lên Kafka topic order-created-events.
     * Chờ ACK đồng bộ trước khi cập nhật SENT.
     */
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingOutboxEvents() {
        List<OutboxEventEntity> pendingEvents =
                outboxEventRepository.findTop20ByStatusOrderByCreatedAtAsc("PENDING");

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("🚀 [OutboxPublisher] Tìm thấy {} sự kiện PENDING đang chờ phát hành lên Kafka...", pendingEvents.size());

        for (OutboxEventEntity outbox : pendingEvents) {
            try {
                OrderCreatedEvent dto = objectMapper.readValue(outbox.getPayload(), OrderCreatedEvent.class);
                String partitionKey = dto.getCustomerId();

                SendResult<String, String> result = kafkaTemplate.send(
                        KafkaTopicConstants.TOPIC_ORDER_CREATED,
                        partitionKey,
                        outbox.getPayload()
                ).get(5, TimeUnit.SECONDS);

                outbox.setStatus("SENT");
                outbox.setSentAt(LocalDateTime.now());
                outbox.setLastError(null);
                outboxEventRepository.save(outbox);

                log.info("✅ [Outbox -> Kafka] Đã đẩy thành công event {} (Order: {}) vào Partition: {}, Offset: {}",
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
                    log.error("💥 [OutboxPublisher] Event {} đạt giới hạn retry tối đa ({}/{}). Đánh dấu FAILED_MAX_RETRIES!",
                            outbox.getEventId(), outbox.getRetryCount(), MAX_RETRIES);
                } else {
                    log.warn("⚠️ [OutboxPublisher] Gửi Kafka thất bại lần {}/{} cho event {}: {}",
                            outbox.getRetryCount(), MAX_RETRIES, outbox.getEventId(), errorMsg);
                }
                outboxEventRepository.save(outbox);
            }
        }
    }
}

