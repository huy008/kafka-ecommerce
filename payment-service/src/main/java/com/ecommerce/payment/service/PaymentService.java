package com.ecommerce.payment.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.ecommerce.payment.entity.PaymentEntity;
import com.ecommerce.payment.entity.ProcessedEventEntity;
import com.ecommerce.payment.repository.PaymentRepository;
import com.ecommerce.payment.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * SAGA CHOREOGRAPHY: Processes payment upon receiving InventoryReservedEvent.
     */
    @Transactional
    public void processPayment(InventoryReservedEvent event) {
        String eventId = event.getEventId();

        // 1. IDEMPOTENT GUARD
        if (processedEventRepository.existsById(eventId)) {
            log.warn("🛡️ [Payment Idempotent Guard] Event {} already charged previously. Skipping duplicate charge!", eventId);
            return;
        }

        String paymentId = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("💳 [PaymentService] Processing payment for order: {} | Amount: {} VND | Method: {}",
                event.getOrderNumber(), event.getTotalAmount(), event.getPaymentMethod());

        // 2. SIMULATE PAYMENT GATEWAY REJECTION (If customer ID requests failure or amount exceeds 200M)
        boolean paymentFailed = (event.getCustomerId() != null && event.getCustomerId().contains("FAIL_PAYMENT"))
                || (event.getTotalAmount() != null && event.getTotalAmount() > 200_000_000.0);

        if (paymentFailed) {
            String failureReason = "Payment gateway declined: Insufficient credit limit or account locked!";
            log.warn("❌ [PaymentService] Payment FAILED for order: {}. Reason: {}", event.getOrderNumber(), failureReason);

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

            // Publish PaymentFailedEvent
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

            // TRIGGER SAGA COMPENSATION: Send InventoryReleaseCommand to release reserved stock in warehouse!
            log.warn("⚡ [Saga Compensating Trigger] Fired inventory release compensation command for order: {}!", event.getOrderNumber());
            InventoryReleaseCommand releaseCmd = new InventoryReleaseCommand(
                    "EVT-COMP-REL-" + UUID.randomUUID().toString().substring(0, 8),
                    event.getOrderNumber(),
                    event.getProductName(),
                    event.getQuantity(),
                    "Payment failed: " + failureReason
            );
            sendKafkaEvent(KafkaTopicConstants.TOPIC_INVENTORY_RELEASE, event.getCustomerId(), releaseCmd);

        } else {
            // PAYMENT SUCCESSFUL
            log.info("✅ [PaymentService] Payment SUCCESSFUL for order: {}. Transaction ID: {}", event.getOrderNumber(), paymentId);

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

            // Publish PaymentSuccessEvent
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

        // Save Idempotency record
        processedEventRepository.save(new ProcessedEventEntity(eventId, "PaymentService"));
    }

    private void sendKafkaEvent(String topic, String key, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, key, json);
        } catch (Exception e) {
            log.error("Error sending Kafka event to topic {}: {}", topic, e.getMessage());
        }
    }

    public List<PaymentEntity> getRecentPayments() {
        return paymentRepository.findTop50ByOrderByCreatedAtDesc();
    }
}
