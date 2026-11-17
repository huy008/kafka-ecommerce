package com.ecommerce.voucher.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.InventoryFailedEvent;
import com.ecommerce.common.event.PaymentFailedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class VoucherEventListener {

    private final VoucherService voucherService;
    private final ObjectMapper objectMapper;

    /**
     * SAGA COMPENSATION: Listens for PaymentFailedEvent -> Releases used voucher quota!
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_PAYMENT_FAILED, groupId = "voucher-service-saga-group")
    public void onPaymentFailed(ConsumerRecord<String, String> record) {
        try {
            PaymentFailedEvent event = objectMapper.readValue(record.value(), PaymentFailedEvent.class);
            log.warn("🔄 [Voucher Saga Compensator] Received PaymentFailedEvent for order: {}. Releasing voucher quota...",
                    event.getOrderNumber());
            voucherService.releaseVoucherCompensation(event.getOrderNumber(), "Payment failed: " + event.getReason());
        } catch (Exception e) {
            log.error("Error processing PaymentFailedEvent in Voucher Service: {}", e.getMessage());
        }
    }

    /**
     * SAGA COMPENSATION: Listens for InventoryFailedEvent -> Releases used voucher quota!
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_FAILED, groupId = "voucher-service-saga-group")
    public void onInventoryFailed(ConsumerRecord<String, String> record) {
        try {
            InventoryFailedEvent event = objectMapper.readValue(record.value(), InventoryFailedEvent.class);
            log.warn("🔄 [Voucher Saga Compensator] Received InventoryFailedEvent for order: {}. Releasing voucher quota...",
                    event.getOrderNumber());
            voucherService.releaseVoucherCompensation(event.getOrderNumber(), "Inventory out of stock: " + event.getReason());
        } catch (Exception e) {
            log.error("Error processing InventoryFailedEvent in Voucher Service: {}", e.getMessage());
        }
    }
}
