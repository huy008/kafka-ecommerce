package com.ecommerce.payment.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.InventoryReservedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventListener {

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

    /**
     * Listens for InventoryReservedEvent -> Proceed to charge customer
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_RESERVED, groupId = "payment-service-group")
    public void onInventoryReserved(ConsumerRecord<String, String> record) {
        try {
            InventoryReservedEvent event = objectMapper.readValue(record.value(), InventoryReservedEvent.class);
            paymentService.processPayment(event);
        } catch (Exception e) {
            log.error("Error processing InventoryReservedEvent in Payment Service: {}", e.getMessage());
        }
    }
}
