package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.DeadLetterEventEntity;
import com.example.ecommerce.repository.DeadLetterEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class DeadLetterQueueService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueService.class);

    private final DeadLetterEventRepository deadLetterEventRepository;
    private final OrderService orderService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public DeadLetterQueueService(DeadLetterEventRepository deadLetterEventRepository,
                                  OrderService orderService,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper) {
        this.deadLetterEventRepository = deadLetterEventRepository;
        this.orderService = orderService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public List<DeadLetterEventEntity> getRecentDeadLetters() {
        return deadLetterEventRepository.findTop50ByOrderByReceivedAtDesc();
    }

    public Optional<DeadLetterEventEntity> getDeadLetterById(Long id) {
        return deadLetterEventRepository.findById(id);
    }

    /**
     * REPLAY PATTERN (Xử lý khắc phục lỗi DLQ):
     * Khi hàng đã được bổ sung vào kho, nhân viên vận hành (Ops/Admin)
     * nhấn nút Replay. Hệ thống sửa đổi thông tin sản phẩm hợp lệ, cấp eventId mới
     * và đẩy lại vào topic 'order-events' để tiếp tục luồng xử lý tự động!
     */
    @Transactional
    public boolean replayDeadLetter(Long id) {
        Optional<DeadLetterEventEntity> optionalDlt = deadLetterEventRepository.findById(id);
        if (!optionalDlt.isPresent()) {
            return false;
        }

        DeadLetterEventEntity dlt = optionalDlt.get();
        try {
            OrderEventDTO dto = objectMapper.readValue(dlt.getPayload(), OrderEventDTO.class);

            // Cập nhật tên sản phẩm nếu đang chứa chuỗi lỗi FAIL_INVENTORY để qua được validate kho
            if (dto.getProductName() != null && dto.getProductName().contains("FAIL_INVENTORY")) {
                dto.setProductName(dto.getProductName().replace("FAIL_INVENTORY_", "RESTOCKED_"));
            }

            // Sinh eventId mới để vượt qua tầng Idempotent Guard
            String newEventId = "EVT-REPLAY-" + UUID.randomUUID().toString().substring(0, 8);
            dto.setEventId(newEventId);
            dto.setTimestamp(LocalDateTime.now().toString());

            String newPayload = objectMapper.writeValueAsString(dto);

            // Đổi trạng thái đơn hàng về PENDING
            orderService.updateOrderStatus(dto.getOrderNumber(), "PENDING", "Đang xử lý lại sau khi Replay từ DLQ");

            // Bắn lại vào Kafka topic order-events
            kafkaTemplate.send(KafkaTopicAndStreamsConfig.TOPIC_ORDERS, dto.getCustomerId(), newPayload);

            // Đánh dấu bản ghi DLQ là REPLAYED
            dlt.setStatus("REPLAYED");
            dlt.setReplayedAt(LocalDateTime.now());
            deadLetterEventRepository.save(dlt);

            log.info("🔁 [DLQ Replay] Đã phát lại thành công đơn {} với eventId mới {} lên topic {}",
                    dto.getOrderNumber(), newEventId, KafkaTopicAndStreamsConfig.TOPIC_ORDERS);

            return true;
        } catch (Exception e) {
            log.error("❌ [DLQ Replay] Lỗi khi replay bản tin ID {}: {}", id, e.getMessage());
            return false;
        }
    }
}

