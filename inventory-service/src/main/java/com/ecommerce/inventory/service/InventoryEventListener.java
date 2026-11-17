package com.ecommerce.inventory.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.InventoryReleaseCommand;
import com.ecommerce.common.event.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventListener {

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    /**
     * Listens for new orders created from Order Service
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_ORDER_CREATED, groupId = "inventory-service-group")
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        try {
            OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
            inventoryService.reserveStock(event);
        } catch (Exception e) {
            log.error("Error processing OrderCreatedEvent in Inventory Service: {}", e.getMessage());
        }
    }

    /**
     * Listens for inventory release commands (Saga Compensating Transaction) from Payment Service when payment fails
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_RELEASE, groupId = "inventory-service-compensation-group")
    public void onInventoryRelease(ConsumerRecord<String, String> record) {
        try {
            InventoryReleaseCommand cmd = objectMapper.readValue(record.value(), InventoryReleaseCommand.class);
            inventoryService.releaseStock(cmd);
        } catch (Exception e) {
            log.error("Error processing InventoryReleaseCommand: {}", e.getMessage());
        }
    }
}
