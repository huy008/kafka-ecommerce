package com.ecommerce.payment.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.ecommerce.payment.entity.PaymentEntity;
import com.ecommerce.payment.entity.ProcessedEventEntity;
import com.ecommerce.payment.repository.PaymentRepository;
import com.ecommerce.payment.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentService(PaymentRepository paymentRepository,
                          ProcessedEventRepository processedEventRepository,
                          KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.processedEventRepository = processedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * SAGA CHOREOGRAPHY: Thực hiện trừ tiền khi nhận InventoryReservedEvent
     */
    @Transactional
    public void processPayment(InventoryReservedEvent event) {
        String eventId = event.getEventId();

        // 1. IDEMPOTENT GUARD
        if (processedEventRepository.existsById(eventId)) {
            log.warn("🛡️ [Payment Idempotent Guard] Event {} đã trừ tiền trước đó. Bỏ qua tránh trừ tiền lần hai!", eventId);
            return;
        }

        String paymentId = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("💳 [PaymentService] Đang xử lý thanh toán cho đơn: {} | Số tiền: {} VND | Phương thức: {}",
                event.getOrderNumber(), event.getTotalAmount(), event.getPaymentMethod());

        // 2. GIẢ LẬP LỖI CỔNG THANH TOÁN (Nếu user cố tình chọn mã lỗi hoặc số tiền quá lớn)
        boolean paymentFailed = (event.getCustomerId() != null && event.getCustomerId().contains("FAIL_PAYMENT"))
                || (event.getTotalAmount() != null && event.getTotalAmount() > 200_000_000.0);

        if (paymentFailed) {
            String failureReason = "Cổng thanh toán từ chối: Thẻ không đủ hạn mức hoặc tài khoản bị khóa!";
            log.warn("❌ [PaymentService] Thanh toán THẤT BẠI cho đơn: {}. Lý do: {}", event.getOrderNumber(), failureReason);

            PaymentEntity payment = new PaymentEntity(
                    paymentId,
                    event.getOrderNumber(),
                    event.getCustomerId(),
                    event.getTotalAmount(),
                    event.getPaymentMethod(),
                    "FAILED",
                    failureReason
            );
            paymentRepository.save(payment);

            // Bắn PaymentFailedEvent
            PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                    "EVT-PAY-FAIL-" + UUID.randomUUID().toString().substring(0, 8),
                    event.getOrderNumber(),
                    event.getCustomerId(),
                    event.getProductName(),
                    event.getQuantity(),
                    event.getTotalAmount(),
                    failureReason
            );
            sendKafkaEvent(KafkaTopicConstants.TOPIC_PAYMENT_FAILED, event.getCustomerId(), failedEvent);

            // KÍCH HOẠT LỆNH BÙ TRỪ SAGA: Bắn InventoryReleaseCommand để kho hoàn trả lại sản phẩm đã giữ!
            log.warn("⚡ [Saga Compensating Trigger] Kích hoạt lệnh xả kho bù trừ cho đơn: {}!", event.getOrderNumber());
            InventoryReleaseCommand releaseCmd = new InventoryReleaseCommand(
                    "EVT-COMP-REL-" + UUID.randomUUID().toString().substring(0, 8),
                    event.getOrderNumber(),
                    event.getProductName(),
                    event.getQuantity(),
                    "Thanh toán thất bại: " + failureReason
            );
            sendKafkaEvent(KafkaTopicConstants.TOPIC_INVENTORY_RELEASE, event.getCustomerId(), releaseCmd);

        } else {
            // THANH TOÁN THÀNH CÔNG
            log.info("✅ [PaymentService] Thanh toán THÀNH CÔNG cho đơn: {}. Mã giao dịch: {}", event.getOrderNumber(), paymentId);

            PaymentEntity payment = new PaymentEntity(
                    paymentId,
                    event.getOrderNumber(),
                    event.getCustomerId(),
                    event.getTotalAmount(),
                    event.getPaymentMethod(),
                    "SUCCESS",
                    null
            );
            paymentRepository.save(payment);

            // Bắn PaymentSuccessEvent
            PaymentSuccessEvent successEvent = new PaymentSuccessEvent(
                    "EVT-PAY-SUCC-" + UUID.randomUUID().toString().substring(0, 8),
                    paymentId,
                    event.getOrderNumber(),
                    event.getCustomerId(),
                    event.getTotalAmount(),
                    event.getPaymentMethod()
            );
            sendKafkaEvent(KafkaTopicConstants.TOPIC_PAYMENT_SUCCESS, event.getCustomerId(), successEvent);
        }

        // Lưu Idempotency record
        processedEventRepository.save(new ProcessedEventEntity(eventId, "PaymentService"));
    }

    private void sendKafkaEvent(String topic, String key, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, key, json);
        } catch (Exception e) {
            log.error("Lỗi gửi Kafka event tới topic {}: {}", topic, e.getMessage());
        }
    }

    public List<PaymentEntity> getRecentPayments() {
        return paymentRepository.findTop50ByOrderByCreatedAtDesc();
    }
}

