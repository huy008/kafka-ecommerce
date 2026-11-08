package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.DeadLetterEventEntity;
import com.example.ecommerce.repository.DeadLetterEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderDltConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderDltConsumer.class);

    private final DeadLetterEventRepository deadLetterEventRepository;
    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    public OrderDltConsumer(DeadLetterEventRepository deadLetterEventRepository,
                            OrderService orderService,
                            ObjectMapper objectMapper) {
        this.deadLetterEventRepository = deadLetterEventRepository;
        this.orderService = orderService;
        this.objectMapper = objectMapper;
    }

    /**
     * DLT CONSUMER:
     * Lắng nghe các bản tin bị chuyển vào Dead Letter Topic (order-events.DLT)
     * sau khi Consumer chính đã thử retry 2 lần thất bại.
     * Tự động lưu bản ghi vào Database để Admin có thể kiểm tra và Replay,
     * đồng thời cập nhật trạng thái đơn hàng sang FAILED_INVENTORY.
     */
    @KafkaListener(
            topics = KafkaTopicAndStreamsConfig.TOPIC_ORDERS_DLT,
            groupId = "dlt-monitoring-handler-group"
    )
    @Transactional
    public void processDeadLetter(
            ConsumerRecord<String, String> record,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION_ID, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            @Header(value = "kafka_dlt-exception-message", required = false) String exceptionMessage) {

        String payload = record.value();
        String eventId = "UNKNOWN";
        String orderNumber = "UNKNOWN";

        try {
            OrderEventDTO dto = objectMapper.readValue(payload, OrderEventDTO.class);
            if (dto.getEventId() != null) eventId = dto.getEventId();
            if (dto.getOrderNumber() != null) orderNumber = dto.getOrderNumber();
        } catch (Exception e) {
            log.warn("⚠️ [DLT Consumer] Không thể parse payload JSON: {}", e.getMessage());
        }

        String actualError = (exceptionMessage != null && !exceptionMessage.isEmpty())
                ? exceptionMessage
                : "Quá số lần retry tối đa (Max Retries Exceeded) - Chuyển giao Dead Letter Queue";

        log.error("💀 [DEAD LETTER TOPIC] Tiếp nhận bản tin hỏng từ topic {}: EventId: {}, Order: {}, Error: {}",
                topic, eventId, orderNumber, actualError);

        // 1. Lưu lại bản ghi Dead Letter vào Database
        DeadLetterEventEntity deadLetter = new DeadLetterEventEntity(
                eventId,
                orderNumber,
                record.topic(),
                record.partition(),
                record.offset(),
                payload,
                actualError
        );
        deadLetterEventRepository.save(deadLetter);

        // 2. Cập nhật trạng thái đơn hàng trong bảng orders sang FAILED_INVENTORY
        if (!"UNKNOWN".equals(orderNumber)) {
            orderService.updateOrderStatus(
                    orderNumber,
                    "FAILED_INVENTORY",
                    "Đơn hàng thất bại do lỗi kho (Chuyển vào DLQ): " + actualError
            );
        }
    }
}

