package com.ecommerce.inventory.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.InventoryReleaseCommand;
import com.ecommerce.common.event.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    public InventoryEventListener(InventoryService inventoryService, ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.objectMapper = objectMapper;
    }

    /**
     * Lắng nghe đơn hàng mới được tạo từ Order Service
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_ORDER_CREATED, groupId = "inventory-service-group")
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        try {
            OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
            inventoryService.reserveStock(event);
        } catch (Exception e) {
            log.error("Lỗi khi xử lý OrderCreatedEvent trong Inventory Service: {}", e.getMessage());
        }
    }

    /**
     * Lắng nghe lệnh xả kho (Saga Compensating Transaction) từ Payment Service khi thanh toán thất bại
     */
    @KafkaListener(topics = KafkaTopicConstants.TOPIC_INVENTORY_RELEASE, groupId = "inventory-service-compensation-group")
    public void onInventoryRelease(ConsumerRecord<String, String> record) {
        try {
            InventoryReleaseCommand cmd = objectMapper.readValue(record.value(), InventoryReleaseCommand.class);
            inventoryService.releaseStock(cmd);
        } catch (Exception e) {
            log.error("Lỗi khi xử lý InventoryReleaseCommand: {}", e.getMessage());
        }
    }
}

