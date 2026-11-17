package com.ecommerce.order.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderSagaResponseListener {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    /**
     * 1. When Inventory has successfully reserved stock -> update status to INVENTORY_RESERVED
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_RESERVED, groupId = "order-service-saga-group")
    public void onInventoryReserved(ConsumerRecord<String, String> record) {
        try {
            InventoryReservedEvent event = objectMapper.readValue(record.value(), InventoryReservedEvent.class);
            log.info("📦 [Saga Choreography] Inventory reserved successfully for order: {}", event.getOrderNumber());
            orderService.updateOrderStatus(event.getOrderNumber(), "INVENTORY_RESERVED", null);
        } catch (Exception e) {
            log.error("Error parsing InventoryReservedEvent: {}", e.getMessage());
        }
    }

    /**
     * 2. When Inventory reports OUT OF STOCK -> update status to FAILED_OUT_OF_STOCK
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_FAILED, groupId = "order-service-saga-group")
    public void onInventoryFailed(ConsumerRecord<String, String> record) {
        try {
            InventoryFailedEvent event = objectMapper.readValue(record.value(), InventoryFailedEvent.class);
            log.warn("❌ [Saga Choreography] Inventory out of stock for order: {}. Reason: {}", event.getOrderNumber(), event.getReason());
            orderService.updateOrderStatus(event.getOrderNumber(), "FAILED_OUT_OF_STOCK", event.getReason());
        } catch (Exception e) {
            log.error("Error parsing InventoryFailedEvent: {}", e.getMessage());
        }
    }

    /**
     * 3. When Payment is SUCCESSFUL -> Order is CONFIRMED!
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_PAYMENT_SUCCESS, groupId = "order-service-saga-group")
    public void onPaymentSuccess(ConsumerRecord<String, String> record) {
        try {
            PaymentSuccessEvent event = objectMapper.readValue(record.value(), PaymentSuccessEvent.class);
            log.info("🎉 [Saga Choreography] Payment successful! Confirming order: {} (Tx ID: {})",
                    event.getOrderNumber(), event.getPaymentId());
            orderService.updateOrderStatus(event.getOrderNumber(), "CONFIRMED", null);
        } catch (Exception e) {
            log.error("Error parsing PaymentSuccessEvent: {}", e.getMessage());
        }
    }

    /**
     * 4. When Payment FAILS -> update status to FAILED_PAYMENT
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_PAYMENT_FAILED, groupId = "order-service-saga-group")
    public void onPaymentFailed(ConsumerRecord<String, String> record) {
        try {
            PaymentFailedEvent event = objectMapper.readValue(record.value(), PaymentFailedEvent.class);
            log.warn("❌ [Saga Choreography] Payment failed for order: {}. Reason: {}", event.getOrderNumber(), event.getReason());
            orderService.updateOrderStatus(event.getOrderNumber(), "FAILED_PAYMENT", event.getReason());
        } catch (Exception e) {
            log.error("Error parsing PaymentFailedEvent: {}", e.getMessage());
        }
    }

    /**
     * 5. When Kafka Streams detects FRAUD -> flag order as FRAUD_SUSPECTED
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_FRAUD_ALERTS, groupId = "order-service-saga-group")
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        try {
            FraudAlertEvent event = objectMapper.readValue(record.value(), FraudAlertEvent.class);
            log.warn("🚨 [Saga Defense] Fraud alert received from Kafka Streams for customer: {}. Automatically locking orders!",
                    event.getCustomerId());
            orderService.flagOrderAsFraudSuspicious(event.getCustomerId());
        } catch (Exception e) {
            log.error("Error parsing FraudAlertEvent: {}", e.getMessage());
        }
    }
}
