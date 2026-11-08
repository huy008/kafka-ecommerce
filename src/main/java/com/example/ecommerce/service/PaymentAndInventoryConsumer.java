package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.ProcessedEventEntity;
import com.example.ecommerce.repository.OrderRepository;
import com.example.ecommerce.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentAndInventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentAndInventoryConsumer.class);

    private final ProcessedEventRepository processedEventRepository;
    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentAndInventoryConsumer(ProcessedEventRepository processedEventRepository,
                                       OrderRepository orderRepository,
                                       KafkaTemplate<String, String> kafkaTemplate,
                                       ObjectMapper objectMapper) {
        this.processedEventRepository = processedEventRepository;
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * IDEMPOTENT CONSUMER PATTERN:
     * Dù Kafka có gửi lại message nhiều lần do retry mạng,
     * Consumer kiểm tra bảng processed_events bằng eventId để đảm bảo
     * chỉ trừ tiền và xuất kho ĐÚNG 1 LẦN DUY NHẤT.
     */
    @KafkaListener(
            topics = KafkaTopicAndStreamsConfig.TOPIC_ORDERS,
            groupId = "payment-inventory-group"
    )
    @Transactional
    public void processOrderEvent(ConsumerRecord<String, String> record) {
        OrderEventDTO orderEvent;
        try {
            orderEvent = objectMapper.readValue(record.value(), OrderEventDTO.class);
        } catch (Exception e) {
            log.error("❌ [Consumer] Poison Pill: Không thể deserialize message từ Partition: {}, Offset: {}. Ném lỗi để DLQ tiếp nhận!",
                    record.partition(), record.offset(), e);
            throw new IllegalArgumentException("Poison pill không hợp lệ: " + record.value(), e);
        }

        String eventId = orderEvent.getEventId();

        log.info("📥 [Consumer] Nhận event {} (Đơn hàng: {}) từ Partition: {}, Offset: {}",
                eventId, orderEvent.getOrderNumber(), record.partition(), record.offset());

        // 1. KIỂM TRA TÍNH BẤT BIẾN (IDEMPOTENCY CHECK)
        if (processedEventRepository.existsById(eventId)) {
            log.warn("🛡️ [Idempotent Guard] Event {} ĐÃ ĐƯỢC XỬ LÝ TRƯỚC ĐÓ! Bỏ qua ngay lập tức để tránh trừ tiền 2 lần.", eventId);
            return;
        }

        // 2. GIẢ LẬP LỖI NGHIỆP VỤ ĐỂ TEST DEAD LETTER QUEUE (DLQ)
        if (orderEvent.getProductName() != null && orderEvent.getProductName().contains("FAIL_INVENTORY")) {
            log.error("💥 [Nghiệp vụ lỗi] Sản phẩm {} đã hết hàng trong kho! Ném lỗi để kích hoạt DefaultErrorHandler Retry & DLT...",
                    orderEvent.getProductName());
            throw new RuntimeException("Kho báo hết hàng đối với mã đơn: " + orderEvent.getOrderNumber());
        }

        // 3. XỬ LÝ THANH TOÁN & XUẤT KHO THÀNH CÔNG
        log.info("💳 [Thanh toán & Xuất kho] Trừ tiền thành công cho đơn {} - Số tiền: {} VND ({})",
                orderEvent.getOrderNumber(), orderEvent.getAmount(), orderEvent.getPaymentMethod());

        // Lưu dấu vết vào bảng processed_events để đảm bảo tính bất biến (Idempotent)
        processedEventRepository.save(new ProcessedEventEntity(eventId, "PaymentAndInventoryService"));

        // Cập nhật trạng thái đơn hàng sang CONFIRMED
        orderRepository.findByOrderNumber(orderEvent.getOrderNumber()).ifPresent(order -> {
            order.setStatus("CONFIRMED");
            orderRepository.save(order);
        });

        // Gửi thông báo thanh toán thành công
        String confirmPayload = String.format("{\"orderNumber\":\"%s\",\"customerId\":\"%s\",\"status\":\"PAID_AND_CONFIRMED\"}",
                orderEvent.getOrderNumber(), orderEvent.getCustomerId());
        kafkaTemplate.send(KafkaTopicAndStreamsConfig.TOPIC_PAYMENT_CONFIRMED, orderEvent.getCustomerId(), confirmPayload);

        log.info("🎉 [Hoàn tất đơn hàng] Đơn hàng {} đã được xác nhận thanh toán & xuất kho thành công!", orderEvent.getOrderNumber());
    }
}
