package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.OutboxEventEntity;
import com.example.ecommerce.repository.OutboxEventRepository;
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
     * Chạy nền định kỳ mỗi 1 giây: Quét các bản tin PENDING trong outbox_events và đẩy sang Kafka.
     * Đảm bảo nguyên tắc At-Least-Once Delivery (Không bao giờ mất bản tin).
     * Chỉ đánh dấu SENT khi Kafka Broker đã gửi xác nhận ACK thành công!
     */
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingOutboxEvents() {
        List<OutboxEventEntity> pendingEvents =
                outboxEventRepository.findTop20ByStatusOrderByCreatedAtAsc("PENDING");

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("🚀 [Outbox Publisher] Tìm thấy {} sự kiện PENDING đang chờ phát hành lên Kafka...", pendingEvents.size());

        for (OutboxEventEntity outbox : pendingEvents) {
            try {
                // Đọc DTO để lấy customerId làm Partition Key
                OrderEventDTO dto = objectMapper.readValue(outbox.getPayload(), OrderEventDTO.class);
                String partitionKey = dto.getCustomerId();

                // Gửi đồng bộ có timeout để bảo đảm Kafka Broker đã ACK trước khi cập nhật DB
                SendResult<String, String> result = kafkaTemplate.send(
                        KafkaTopicAndStreamsConfig.TOPIC_ORDERS,
                        partitionKey,
                        outbox.getPayload()
                ).get(5, TimeUnit.SECONDS);

                // Gửi thành công -> Cập nhật trạng thái SENT
                outbox.setStatus("SENT");
                outbox.setSentAt(LocalDateTime.now());
                outbox.setLastError(null);
                outboxEventRepository.save(outbox);

                log.info("✅ [Outbox -> Kafka] Đã đẩy thành công event {} (Key: {}) vào Partition: {}, Offset: {}",
                        outbox.getEventId(),
                        partitionKey,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());

            } catch (Exception e) {
                outbox.incrementRetryCount();
                String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                outbox.setLastError(errorMsg);

                if (outbox.getRetryCount() >= MAX_RETRIES) {
                    outbox.setStatus("FAILED_MAX_RETRIES");
                    log.error("💥 [Outbox Publisher] Event {} đạt giới hạn retry tối đa ({}/{}). Đánh dấu FAILED_MAX_RETRIES!",
                            outbox.getEventId(), outbox.getRetryCount(), MAX_RETRIES);
                } else {
                    log.warn("⚠️ [Outbox Publisher] Gửi Kafka thất bại lần {}/{} cho event {}: {}. Sẽ thử lại trong lượt quét kế tiếp.",
                            outbox.getRetryCount(), MAX_RETRIES, outbox.getEventId(), errorMsg);
                }
                outboxEventRepository.save(outbox);
            }
        }
    }

    /**
     * Cho phép Admin kích hoạt lại các bản tin Outbox bị kẹt / lỗi
     */
    @Transactional
    public int retryAllFailedOutboxEvents() {
        List<OutboxEventEntity> failedList = outboxEventRepository.findByStatus("FAILED_MAX_RETRIES");
        for (OutboxEventEntity evt : failedList) {
            evt.setStatus("PENDING");
            evt.setRetryCount(0);
            evt.setLastError("Admin thủ công reset trạng thái để retry");
            outboxEventRepository.save(evt);
        }
        log.info("🔄 [Outbox Admin] Đã reset {} sự kiện FAILED_MAX_RETRIES về PENDING để gửi lại", failedList.size());
        return failedList.size();
    }
}
