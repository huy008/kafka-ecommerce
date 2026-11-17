package com.ecommerce.notification.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationEventListener {

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    /**
     * When payment is successful -> Send order confirmation & e-invoice notification
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_PAYMENT_SUCCESS, groupId = "notification-service-group")
    public void onPaymentSuccess(ConsumerRecord<String, String> record) {
        try {
            PaymentSuccessEvent event = objectMapper.readValue(record.value(), PaymentSuccessEvent.class);
            notificationService.sendNotification(
                    "ORDER_CONFIRMED",
                    event.getCustomerId(),
                    "Order " + event.getOrderNumber() + " has been confirmed!",
                    "Payment of " + String.format("%,.0f", event.getAmount()) + " VND via " + event.getPaymentMethod() + " succeeded. Your order is being packed."
            );
        } catch (Exception e) {
            log.error("Error processing PaymentSuccessEvent: {}", e.getMessage());
        }
    }

    /**
     * When inventory runs out of stock -> Send apology & cancellation notification
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_FAILED, groupId = "notification-service-group")
    public void onInventoryFailed(ConsumerRecord<String, String> record) {
        try {
            InventoryFailedEvent event = objectMapper.readValue(record.value(), InventoryFailedEvent.class);
            notificationService.sendNotification(
                    "OUT_OF_STOCK",
                    event.getCustomerId(),
                    "Order " + event.getOrderNumber() + " could not be completed due to out of stock!",
                    "We are sorry, product '" + event.getProductName() + "' is currently out of stock. We apologize for the inconvenience."
            );
        } catch (Exception e) {
            log.error("Error processing InventoryFailedEvent: {}", e.getMessage());
        }
    }

    /**
     * When fraud is detected -> Send security alert notification
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_FRAUD_ALERTS, groupId = "notification-service-group")
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        try {
            FraudAlertEvent event = objectMapper.readValue(record.value(), FraudAlertEvent.class);
            notificationService.sendNotification(
                    "SECURITY_ALERT",
                    event.getCustomerId(),
                    "Security Warning: Suspicious activity detected on your account!",
                    event.getAlertReason()
            );
        } catch (Exception e) {
            log.error("Error processing FraudAlertEvent: {}", e.getMessage());
        }
    }
}
