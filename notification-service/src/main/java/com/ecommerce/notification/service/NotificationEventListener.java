package com.ecommerce.notification.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationEventListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventListener.class);

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    public NotificationEventListener(NotificationService notificationService, ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
    }

    /**
     * Khi thanh toán thành công -> Gửi thông báo xác nhận đơn hàng & hóa đơn điện tử
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_PAYMENT_SUCCESS, groupId = "notification-service-group")
    public void onPaymentSuccess(ConsumerRecord<String, String> record) {
        try {
            PaymentSuccessEvent event = objectMapper.readValue(record.value(), PaymentSuccessEvent.class);
            notificationService.sendNotification(
                    "ORDER_CONFIRMED",
                    event.getCustomerId(),
                    "Đơn hàng " + event.getOrderNumber() + " đã được xác nhận!",
                    "Thanh toán " + String.format("%,.0f", event.getAmount()) + " VND thành công qua " + event.getPaymentMethod() + ". Đơn hàng đang được đóng gói."
            );
        } catch (Exception e) {
            log.error("Lỗi xử lý PaymentSuccessEvent: {}", e.getMessage());
        }
    }

    /**
     * Khi hết hàng trong kho -> Gửi thông báo xin lỗi khách hàng
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_FAILED, groupId = "notification-service-group")
    public void onInventoryFailed(ConsumerRecord<String, String> record) {
        try {
            InventoryFailedEvent event = objectMapper.readValue(record.value(), InventoryFailedEvent.class);
            notificationService.sendNotification(
                    "OUT_OF_STOCK",
                    event.getCustomerId(),
                    "Đơn hàng " + event.getOrderNumber() + " không thể hoàn tất do hết hàng!",
                    "Rất tiếc sản phẩm '" + event.getProductName() + "' hiện đã hết hàng trong kho. Xin quý khách thông cảm."
            );
        } catch (Exception e) {
            log.error("Lỗi xử lý InventoryFailedEvent: {}", e.getMessage());
        }
    }

    /**
     * Khi phát hiện gian lận -> Gửi cảnh báo bảo mật
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_FRAUD_ALERTS, groupId = "notification-service-group")
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        try {
            FraudAlertEvent event = objectMapper.readValue(record.value(), FraudAlertEvent.class);
            notificationService.sendNotification(
                    "SECURITY_ALERT",
                    event.getCustomerId(),
                    "Cảnh báo bảo mật tài khoản: Phát hiện hành vi bất thường!",
                    event.getAlertReason()
            );
        } catch (Exception e) {
            log.error("Lỗi xử lý FraudAlertEvent: {}", e.getMessage());
        }
    }
}

