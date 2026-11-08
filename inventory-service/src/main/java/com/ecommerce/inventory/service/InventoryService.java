package com.ecommerce.inventory.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.*;
import com.ecommerce.inventory.entity.InventoryStockEntity;
import com.ecommerce.inventory.entity.ProcessedEventEntity;
import com.ecommerce.inventory.repository.InventoryStockRepository;
import com.ecommerce.inventory.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryStockRepository stockRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public InventoryService(InventoryStockRepository stockRepository,
                            ProcessedEventRepository processedEventRepository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper) {
        this.stockRepository = stockRepository;
        this.processedEventRepository = processedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void seedInitialStock() {
        if (stockRepository.count() == 0) {
            stockRepository.save(new InventoryStockEntity("MacBook Pro M3 Max", 20));
            stockRepository.save(new InventoryStockEntity("iPhone 16 Pro Max", 30));
            stockRepository.save(new InventoryStockEntity("PlayStation 5 Pro", 15));
            stockRepository.save(new InventoryStockEntity("Laptop Dell XPS 15", 25));
            stockRepository.save(new InventoryStockEntity("Tai nghe Sony WH-1000XM5", 40));
            stockRepository.save(new InventoryStockEntity("Bàn phím cơ Keychron", 50));
            stockRepository.save(new InventoryStockEntity("FAIL_INVENTORY_PRODUCT", 0)); // Luôn hết hàng để test kịch bản lỗi
            log.info("📦 [InventoryService] Đã khởi tạo dữ liệu tồn kho ban đầu!");
        }
    }

    /**
     * SAGA CHOREOGRAPHY: Khóa và giữ chỗ tồn kho khi nhận OrderCreatedEvent
     */
    @Transactional
    public void reserveStock(OrderCreatedEvent orderEvent) {
        String eventId = orderEvent.getEventId();

        // 1. IDEMPOTENT GUARD
        if (processedEventRepository.existsById(eventId)) {
            log.warn("🛡️ [Inventory Idempotent Guard] Event {} đã xử lý trước đó. Bỏ qua!", eventId);
            return;
        }

        log.info("📦 [InventoryService] Kiểm tra tồn kho cho đơn: {} | Sản phẩm: {} | SL: {}",
                orderEvent.getOrderNumber(), orderEvent.getProductName(), orderEvent.getQuantity());

        // 2. Kiểm tra hàng lỗi kịch bản
        if (orderEvent.getProductName() != null && orderEvent.getProductName().contains("FAIL_INVENTORY")) {
            sendInventoryFailed(orderEvent, "Sản phẩm không khả dụng hoặc đã hết hàng trong kho!");
            processedEventRepository.save(new ProcessedEventEntity(eventId, "InventoryService"));
            return;
        }

        // 3. Tìm sản phẩm trong kho
        Optional<InventoryStockEntity> stockOpt = stockRepository.findByProductName(orderEvent.getProductName());
        if (stockOpt.isPresent()) {
            InventoryStockEntity stock = stockOpt.get();
            int qty = orderEvent.getQuantity() != null ? orderEvent.getQuantity() : 1;

            if (stock.reserve(qty)) {
                stockRepository.save(stock);
                log.info("✅ [InventoryService] Giữ kho THÀNH CÔNG cho đơn: {}. Còn lại khả dụng: {}",
                        orderEvent.getOrderNumber(), stock.getAvailableStock());

                // Bắn event InventoryReservedEvent sang Kafka
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
                sendInventoryFailed(orderEvent, "Số lượng trong kho không đủ (Hiện có: " + stock.getAvailableStock() + ", yêu cầu: " + qty + ")");
            }
        } else {
            // Sản phẩm chưa có trong danh mục kho -> Tự động khởi tạo tồn kho 10 chiếc để hỗ trợ đơn tùy chỉnh
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

        // Đánh dấu đã xử lý
        processedEventRepository.save(new ProcessedEventEntity(eventId, "InventoryService"));
    }

    /**
     * SAGA COMPENSATING TRANSACTION: Xả kho hoàn trả lại số lượng khi thanh toán thất bại
     */
    @Transactional
    public void releaseStock(InventoryReleaseCommand cmd) {
        log.warn("🔄 [Saga Compensation] Thực hiện lệnh BÙ TRỪ XẢ KHO cho đơn: {} | Sản phẩm: {} | SL: {}",
                cmd.getOrderNumber(), cmd.getProductName(), cmd.getQuantity());

        stockRepository.findByProductName(cmd.getProductName()).ifPresent(stock -> {
            stock.release(cmd.getQuantity());
            stockRepository.save(stock);
            log.info("✅ [Saga Compensation] Đã hoàn trả {} sản phẩm '{}' vào kho khả dụng (Mới: {})",
                    cmd.getQuantity(), cmd.getProductName(), stock.getAvailableStock());
        });
    }

    private void sendInventoryFailed(OrderCreatedEvent orderEvent, String reason) {
        log.warn("❌ [InventoryService] Kho TỪ CHỐI đơn: {}. Lý do: {}", orderEvent.getOrderNumber(), reason);
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
            log.error("Lỗi gửi Kafka event tới topic {}: {}", topic, e.getMessage());
        }
    }

    public List<InventoryStockEntity> getAllStocks() {
        return stockRepository.findAll();
    }
}

