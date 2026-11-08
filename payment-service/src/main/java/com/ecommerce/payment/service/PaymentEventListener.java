package com.ecommerce.payment.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.InventoryReservedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

    public PaymentEventListener(PaymentService paymentService, ObjectMapper objectMapper) {
        this.paymentService = paymentService;
        this.objectMapper = objectMapper;
    }

    /**
     * Lắng nghe khi Kho đã giữ hàng thành công -> Tiến hành trừ tiền
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_RESERVED, groupId = "payment-service-group")
    public void onInventoryReserved(ConsumerRecord<String, String> record) {
        try {
            InventoryReservedEvent event = objectMapper.readValue(record.value(), InventoryReservedEvent.class);
            paymentService.processPayment(event);
        } catch (Exception e) {
            log.error("Lỗi khi xử lý InventoryReservedEvent trong Payment Service: {}", e.getMessage());
        }
    }
}

