package com.ecommerce.order.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderSagaResponseListener {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaResponseListener.class);

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    public OrderSagaResponseListener(OrderService orderService, ObjectMapper objectMapper) {
        this.orderService = orderService;
        this.objectMapper = objectMapper;
    }

    /**
     * 1. Khi Kho đã giữ hàng thành công
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_RESERVED, groupId = "order-service-saga-group")
    public void onInventoryReserved(ConsumerRecord<String, String> record) {
        try {
            InventoryReservedEvent event = objectMapper.readValue(record.value(), InventoryReservedEvent.class);
            log.info("📦 [Saga Choreography] Kho đã giữ hàng thành công cho đơn: {}", event.getOrderNumber());
            orderService.updateOrderStatus(event.getOrderNumber(), "INVENTORY_RESERVED", null);
        } catch (Exception e) {
            log.error("Lỗi parse InventoryReservedEvent: {}", e.getMessage());
        }
    }

    /**
     * 2. Khi Kho báo HẾT HÀNG
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_FAILED, groupId = "order-service-saga-group")
    public void onInventoryFailed(ConsumerRecord<String, String> record) {
        try {
            InventoryFailedEvent event = objectMapper.readValue(record.value(), InventoryFailedEvent.class);
            log.warn("❌ [Saga Choreography] Kho báo hết hàng cho đơn: {}. Lý do: {}", event.getOrderNumber(), event.getReason());
            orderService.updateOrderStatus(event.getOrderNumber(), "FAILED_OUT_OF_STOCK", event.getReason());
        } catch (Exception e) {
            log.error("Lỗi parse InventoryFailedEvent: {}", e.getMessage());
        }
    }

    /**
     * 3. Khi Thanh Toán THÀNH CÔNG -> Đơn hàng CONFIRMED!
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_PAYMENT_SUCCESS, groupId = "order-service-saga-group")
    public void onPaymentSuccess(ConsumerRecord<String, String> record) {
        try {
            PaymentSuccessEvent event = objectMapper.readValue(record.value(), PaymentSuccessEvent.class);
            log.info("🎉 [Saga Choreography] Thanh toán thành công! Xác nhận đơn hàng: {} (Mã GD: {})",
                    event.getOrderNumber(), event.getPaymentId());
            orderService.updateOrderStatus(event.getOrderNumber(), "CONFIRMED", null);
        } catch (Exception e) {
            log.error("Lỗi parse PaymentSuccessEvent: {}", e.getMessage());
        }
    }

    /**
     * 4. Khi Thanh Toán THẤT BẠI
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_PAYMENT_FAILED, groupId = "order-service-saga-group")
    public void onPaymentFailed(ConsumerRecord<String, String> record) {
        try {
            PaymentFailedEvent event = objectMapper.readValue(record.value(), PaymentFailedEvent.class);
            log.warn("❌ [Saga Choreography] Thanh toán thất bại cho đơn: {}. Lý do: {}", event.getOrderNumber(), event.getReason());
            orderService.updateOrderStatus(event.getOrderNumber(), "FAILED_PAYMENT", event.getReason());
        } catch (Exception e) {
            log.error("Lỗi parse PaymentFailedEvent: {}", e.getMessage());
        }
    }

    /**
     * 5. Khi Kafka Streams phát hiện GIAN LẬN
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_FRAUD_ALERTS, groupId = "order-service-saga-group")
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        try {
            FraudAlertEvent event = objectMapper.readValue(record.value(), FraudAlertEvent.class);
            log.warn("🚨 [Saga Defense] Cảnh báo gian lận từ Kafka Streams cho khách: {}. Tự động khóa đơn!", event.getCustomerId());
            orderService.flagOrderAsFraudSuspicious(event.getCustomerId());
        } catch (Exception e) {
            log.error("Lỗi parse FraudAlertEvent: {}", e.getMessage());
        }
    }
}

