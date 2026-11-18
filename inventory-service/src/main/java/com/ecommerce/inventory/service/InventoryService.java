package com.ecommerce.inventory.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.ecommerce.inventory.entity.InventoryStockEntity;
import com.ecommerce.inventory.entity.ProcessedEventEntity;
import com.ecommerce.inventory.repository.InventoryStockRepository;
import com.ecommerce.inventory.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryStockRepository stockRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @PostConstruct
    public void seedInitialStock() {
        if (stockRepository.count() == 0) {
            stockRepository.save(new InventoryStockEntity("MacBook Pro M3 Max", 20));
            stockRepository.save(new InventoryStockEntity("iPhone 16 Pro Max", 30));
            stockRepository.save(new InventoryStockEntity("PlayStation 5 Pro", 15));
            stockRepository.save(new InventoryStockEntity("Laptop Dell XPS 15", 25));
            stockRepository.save(new InventoryStockEntity("Tai nghe Sony WH-1000XM5", 40));
            stockRepository.save(new InventoryStockEntity("Bàn phím cơ Keychron", 50));
            stockRepository.save(new InventoryStockEntity("FAIL_INVENTORY_PRODUCT", 0)); // Always out-of-stock for error scenario testing
            log.info("📦 [InventoryService] Initial stock data successfully seeded!");
        }
    }

    /**
     * SAGA CHOREOGRAPHY: Locks and reserves inventory upon receiving OrderCreatedEvent.
     */
    @Transactional
    public void reserveStock(OrderCreatedEvent orderEvent) {
        String eventId = orderEvent.getEventId();

        // 1. IDEMPOTENT GUARD
        if (processedEventRepository.existsById(eventId)) {
            log.warn("🛡️ [Inventory Idempotent Guard] Event {} already processed. Skipping!", eventId);
            return;
        }

        log.info("📦 [InventoryService] Checking stock for order: {} | Product: {} | Qty: {}",
                orderEvent.getOrderNumber(), orderEvent.getProductName(), orderEvent.getQuantity());

        // 2. Check for simulated failure scenario
        if (orderEvent.getProductName() != null && orderEvent.getProductName().contains("FAIL_INVENTORY")) {
            sendInventoryFailed(orderEvent, "Product is unavailable or out of stock!");
            processedEventRepository.save(new ProcessedEventEntity(eventId, "InventoryService"));
            return;
        }

        // 3. Search product in inventory
        Optional<InventoryStockEntity> stockOpt = stockRepository.findByProductName(orderEvent.getProductName());
        if (stockOpt.isPresent()) {
            InventoryStockEntity stock = stockOpt.get();
            int qty = orderEvent.getQuantity() != null ? orderEvent.getQuantity() : 1;

            if (stock.reserve(qty)) {
                stockRepository.save(stock);
                log.info("✅ [InventoryService] Reserved stock SUCCESSFULLY for order: {}. Remaining available: {}",
                        orderEvent.getOrderNumber(), stock.getAvailableStock());

                // Publish InventoryReservedEvent to Kafka
                InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                        "EVT-INV-RES-" + UUID.randomUUID().toString().substring(0, 8),
                        orderEvent.getOrderNumber(),
                        orderEvent.getCustomerId(),
                        orderEvent.getProductName(),
                        qty,
                        orderEvent.getTotalAmount(),
                        orderEvent.getPaymentMethod()
                );
                sendKafkaEvent(KafkaTopicConstants.TOPIC_INVENTORY_RESERVED, orderEvent.getCustomerId(), reservedEvent);
            } else {
                sendInventoryFailed(orderEvent, "Insufficient stock (Available: " + stock.getAvailableStock() + ", Requested: " + qty + ")");
            }
        } else {
            // Product not yet in catalog -> auto-seed with 10 units to support custom testing
            InventoryStockEntity newStock = new InventoryStockEntity(orderEvent.getProductName(), 10);
            int qty = orderEvent.getQuantity() != null ? orderEvent.getQuantity() : 1;
            newStock.reserve(qty);
            stockRepository.save(newStock);

            InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                    "EVT-INV-RES-" + UUID.randomUUID().toString().substring(0, 8),
                    orderEvent.getOrderNumber(),
                    orderEvent.getCustomerId(),
                    orderEvent.getProductName(),
                    qty,
                    orderEvent.getTotalAmount(),
                    orderEvent.getPaymentMethod()
            );
            sendKafkaEvent(KafkaTopicConstants.TOPIC_INVENTORY_RESERVED, orderEvent.getCustomerId(), reservedEvent);
        }

        // Mark event as processed (Idempotency)
        processedEventRepository.save(new ProcessedEventEntity(eventId, "InventoryService"));
    }

    /**
     * SAGA COMPENSATING TRANSACTION: Releases reserved stock back to available stock when payment fails.
     */
    @Transactional
    public void releaseStock(InventoryReleaseCommand cmd) {
        log.warn("🔄 [Saga Compensation] Executing COMPENSATING STOCK RELEASE for order: {} | Product: {} | Qty: {}",
                cmd.getOrderNumber(), cmd.getProductName(), cmd.getQuantity());

        stockRepository.findByProductName(cmd.getProductName()).ifPresent(stock -> {
            stock.release(cmd.getQuantity());
            stockRepository.save(stock);
            log.info("✅ [Saga Compensation] Released {} units of '{}' back to available stock (New Available: {})",
                    cmd.getQuantity(), cmd.getProductName(), stock.getAvailableStock());
        });
    }

    private void sendInventoryFailed(OrderCreatedEvent orderEvent, String reason) {
        log.warn("❌ [InventoryService] REJECTED order: {}. Reason: {}", orderEvent.getOrderNumber(), reason);
        InventoryFailedEvent failedEvent = new InventoryFailedEvent(
                "EVT-INV-FAIL-" + UUID.randomUUID().toString().substring(0, 8),
                orderEvent.getOrderNumber(),
                orderEvent.getCustomerId(),
                orderEvent.getProductName(),
                orderEvent.getQuantity(),
                reason
        );
        sendKafkaEvent(KafkaTopicConstants.TOPIC_INVENTORY_FAILED, orderEvent.getCustomerId(), failedEvent);
    }

    private void sendKafkaEvent(String topic, String key, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, key, json);
        } catch (Exception e) {
            log.error("Error sending Kafka event to topic {}: {}", topic, e.getMessage());
        }
    }

    public List<InventoryStockEntity> getAllStocks() {
        return stockRepository.findAll();
    }
}
