package com.example.ecommerce.controller;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.CreateOrderRequest;
import com.example.ecommerce.dto.DashboardMetricsDTO;
import com.example.ecommerce.dto.FraudAlertEvent;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.DeadLetterEventEntity;
import com.example.ecommerce.entity.OrderEntity;
import com.example.ecommerce.repository.DeadLetterEventRepository;
import com.example.ecommerce.repository.OrderRepository;
import com.example.ecommerce.repository.OutboxEventRepository;
import com.example.ecommerce.repository.ProcessedEventRepository;
import com.example.ecommerce.service.DeadLetterQueueService;
import com.example.ecommerce.service.FraudAlertsListener;
import com.example.ecommerce.service.OrderService;
import com.example.ecommerce.service.OutboxPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/simulation")
@Tag(name = "2. Simulation & Mission Control API", description = "Các kịch bản thực chiến mô phỏng Event-Driven: Outbox, Kafka Streams, Idempotency, DLQ Replay")
public class SimulationAndAnalyticsController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OutboxPublisher outboxPublisher;
    private final ProcessedEventRepository processedEventRepository;
    private final FraudAlertsListener fraudAlertsListener;
    private final DeadLetterEventRepository deadLetterEventRepository;
    private final DeadLetterQueueService deadLetterQueueService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public SimulationAndAnalyticsController(OrderService orderService,
                                            OrderRepository orderRepository,
                                            OutboxEventRepository outboxEventRepository,
                                            OutboxPublisher outboxPublisher,
                                            ProcessedEventRepository processedEventRepository,
                                            FraudAlertsListener fraudAlertsListener,
                                            DeadLetterEventRepository deadLetterEventRepository,
                                            DeadLetterQueueService deadLetterQueueService,
                                            KafkaTemplate<String, String> kafkaTemplate,
                                            ObjectMapper objectMapper) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.outboxPublisher = outboxPublisher;
        this.processedEventRepository = processedEventRepository;
        this.fraudAlertsListener = fraudAlertsListener;
        this.deadLetterEventRepository = deadLetterEventRepository;
        this.deadLetterQueueService = deadLetterQueueService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 1. Mô phỏng lưu lượng bình thường: Tạo nhiều đơn từ các user khác nhau
     */
    @PostMapping("/normal-traffic")
    @Operation(summary = "Kịch bản 1: Giả lập lưu lượng đặt hàng bình thường", description = "Tạo nhiều đơn hàng phân tán trên nhiều user khác nhau để quan sát luồng Outbox -> Kafka -> Consumer.")
    public ResponseEntity<Map<String, Object>> simulateNormalTraffic(
            @RequestParam(defaultValue = "5") int count) {

        List<String> createdOrders = new ArrayList<>();
        String[] sampleProducts = {"MacBook Pro M3", "iPhone 16 Pro Max", "iPad Air M2", "Tai nghe Sony WH-1000XM5", "Bàn phím cơ Keychron"};
        double[] samplePrices = {45000000.0, 34000000.0, 16500000.0, 7500000.0, 2800000.0};

        for (int i = 0; i < count; i++) {
            String customer = "USER_" + (100 + i + (int)(Math.random() * 900));
            int idx = i % sampleProducts.length;
            OrderEntity order = orderService.createOrder(
                    new CreateOrderRequest(customer, sampleProducts[idx], samplePrices[idx])
            );
            createdOrders.add(order.getOrderNumber() + " (" + customer + " - " + sampleProducts[idx] + ")");
        }

        Map<String, Object> res = new HashMap<>();
        res.put("status", "SUCCESS");
        res.put("createdOrdersCount", count);
        res.put("orders", createdOrders);
        res.put("note", "Các đơn hàng phân tán trên nhiều user nên không vi phạm ngưỡng gian lận.");
        return ResponseEntity.ok(res);
    }

    /**
     * 2. Mô phỏng tấn công gian lận / Spam đơn hàng (Kích hoạt Kafka Streams Sliding Window):
     * Gửi liên tiếp >= 4 đơn hàng của CÙNG 1 USER trong 1 giây!
     */
    @PostMapping("/fraud-attack")
    @Operation(summary = "Kịch bản 2: Giả lập tấn công gian lận / Spam đơn hàng", description = "Gửi liên tiếp >= 4 đơn của cùng 1 user trong 1 giây để Kafka Streams phát hiện bằng Sliding Window 30s.")
    public ResponseEntity<Map<String, Object>> simulateFraudAttack(
            @RequestParam(defaultValue = "hacker_99") String customerId,
            @RequestParam(defaultValue = "4") int count) {

        List<String> attackOrders = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            OrderEntity order = orderService.createOrder(
                    new CreateOrderRequest(customerId, "PlayStation 5 Pro Spider-Man Edition", 16500000.0)
            );
            attackOrders.add(order.getOrderNumber());
        }

        Map<String, Object> res = new HashMap<>();
        res.put("status", "ATTACK_SIMULATED");
        res.put("targetUser", customerId);
        res.put("ordersSpawned", attackOrders);
        res.put("expectedBehavior", "Kafka Streams sẽ gom nhóm theo customerId trong Window 30s. Khi nhận >= 3 đơn, phát sinh FraudAlertEvent vào topic 'fraud-alerts' và tự động khóa cảnh báo FRAUD_SUSPECTED!");
        return ResponseEntity.ok(res);
    }

    /**
     * 3. Mô phỏng Poison Pill / Hết hàng để kích hoạt Retry & Dead Letter Queue (DLQ)
     */
    @PostMapping("/dlq-poison-pill")
    @Operation(summary = "Kịch bản 3: Giả lập đơn hàng lỗi kho (Dead Letter Queue)", description = "Đặt đơn chứa sản phẩm 'FAIL_INVENTORY_...' khiến Consumer gặp ngoại lệ -> Retry 2 lần -> Bắn vào topic 'order-events.DLT'.")
    public ResponseEntity<Map<String, Object>> simulateDlqPoisonPill() {
        OrderEntity order = orderService.createOrder(
                new CreateOrderRequest("user_bad_request", "FAIL_INVENTORY_IPHONE_16_LIMITED", 35000000.0)
        );

        Map<String, Object> res = new HashMap<>();
        res.put("status", "POISON_PILL_CREATED");
        res.put("orderNumber", order.getOrderNumber());
        res.put("expectedBehavior", "Payment & Inventory Consumer phát hiện sản phẩm lỗi -> DefaultErrorHandler Retry 2 lần -> Chuyển vào Dead Letter Topic 'order-events.DLT' -> OrderDltConsumer lưu DB và đổi trạng thái sang FAILED_INVENTORY.");
        return ResponseEntity.ok(res);
    }

    /**
     * 4. Thử nghiệm tính bất biến (Idempotent Consumer Guard)
     */
    @PostMapping("/idempotency-test")
    @Operation(summary = "Kịch bản 4: Giả lập Kafka gửi trùng lặp message (Idempotency Test)", description = "Bắn trực tiếp 2 message có cùng eventId vào Kafka topic 'order-events' để chứng minh Consumer chỉ xử lý đúng 1 lần.")
    public ResponseEntity<Map<String, Object>> simulateIdempotencyAttack() throws Exception {
        String duplicateEventId = "EVT-DUPLICATE-TEST-" + UUID.randomUUID().toString().substring(0, 8);
        String testOrderNumber = "ORD-TEST-IDEM-" + UUID.randomUUID().toString().substring(0, 4);

        OrderEventDTO dto = new OrderEventDTO(
                duplicateEventId,
                testOrderNumber,
                "USER_IDEM_TEST",
                "Chuột không dây Logitech MX Master 3S",
                1,
                2490000.0,
                2490000.0,
                "CREDIT_CARD",
                "TP. Hồ Chí Minh"
        );
        String payload = objectMapper.writeValueAsString(dto);

        // Bắn tin nhắn lần 1
        kafkaTemplate.send(KafkaTopicAndStreamsConfig.TOPIC_ORDERS, dto.getCustomerId(), payload);

        // Bắn tin nhắn lần 2 (cùng eventId)
        kafkaTemplate.send(KafkaTopicAndStreamsConfig.TOPIC_ORDERS, dto.getCustomerId(), payload);

        Map<String, Object> res = new HashMap<>();
        res.put("status", "DUPLICATE_MESSAGES_SENT");
        res.put("eventId", duplicateEventId);
        res.put("orderNumber", testOrderNumber);
        res.put("expectedBehavior", "Bản tin 1 được xử lý thành công và lưu vào processed_events. Bản tin 2 bị Idempotent Guard phát hiện trùng lặp và bỏ qua ngay lập tức!");
        return ResponseEntity.ok(res);
    }

    /**
     * 5. Quản lý Dead Letter Queue: Xem danh sách
     */
    @GetMapping("/dlq")
    @Operation(summary = "Xem danh sách bản tin Dead Letter Queue (DLT)")
    public ResponseEntity<List<DeadLetterEventEntity>> getDeadLetters() {
        return ResponseEntity.ok(deadLetterQueueService.getRecentDeadLetters());
    }

    /**
     * 6. Quản lý Dead Letter Queue: Replay bản tin hỏng
     */
    @PostMapping("/dlq/replay/{id}")
    @Operation(summary = "Replay bản tin Dead Letter Queue", description = "Sửa đổi thông tin hợp lệ sau khi kho đã có hàng, cấp eventId mới và đẩy lại vào topic 'order-events'.")
    public ResponseEntity<Map<String, Object>> replayDeadLetter(@PathVariable Long id) {
        boolean success = deadLetterQueueService.replayDeadLetter(id);
        Map<String, Object> res = new HashMap<>();
        res.put("id", id);
        res.put("success", success);
        res.put("message", success ? "Đã replay bản tin thành công lên topic 'order-events'!" : "Không tìm thấy bản ghi hoặc lỗi khi replay.");
        return ResponseEntity.ok(res);
    }

    /**
     * 7. Quản lý Outbox: Thử lại các sự kiện bị lỗi
     */
    @PostMapping("/outbox/retry-failed")
    @Operation(summary = "Thử lại tất cả các bản tin Outbox bị lỗi tối đa retries")
    public ResponseEntity<Map<String, Object>> retryFailedOutbox() {
        int count = outboxPublisher.retryAllFailedOutboxEvents();
        Map<String, Object> res = new HashMap<>();
        res.put("status", "SUCCESS");
        res.put("retriedCount", count);
        res.put("message", "Đã chuyển " + count + " bản tin FAILED_MAX_RETRIES về trạng thái PENDING để Outbox Publisher quét lại.");
        return ResponseEntity.ok(res);
    }

    /**
     * 8. Xem danh sách cảnh báo gian lận từ Kafka Streams
     */
    @GetMapping("/fraud-alerts")
    @Operation(summary = "Danh sách cảnh báo gian lận thời gian thực từ Kafka Streams")
    public ResponseEntity<Map<String, Object>> getFraudAlerts() {
        List<FraudAlertEvent> alerts = fraudAlertsListener.getRecentAlerts();
        Map<String, Object> res = new HashMap<>();
        res.put("totalAlerts", alerts.size());
        res.put("alerts", alerts);
        return ResponseEntity.ok(res);
    }

    @DeleteMapping("/fraud-alerts")
    @Operation(summary = "Xóa lịch sử cảnh báo gian lận")
    public ResponseEntity<Map<String, String>> clearFraudAlerts() {
        fraudAlertsListener.clearAlerts();
        return ResponseEntity.ok(Collections.singletonMap("message", "Đã xóa sạch lịch sử cảnh báo gian lận."));
    }

    /**
     * 9. Dữ liệu tổng hợp toàn diện cho Mission Control Dashboard (Live UI)
     */
    @GetMapping("/dashboard-data")
    @Operation(summary = "Dữ liệu thời gian thực cho Web Dashboard", description = "Cung cấp toàn bộ metrics (Orders, Outbox, Streams, DLQ, Revenue) phục vụ giao diện Web.")
    public ResponseEntity<DashboardMetricsDTO> getDashboardData() {
        DashboardMetricsDTO dto = new DashboardMetricsDTO();

        long totalOrders = orderRepository.count();
        long confirmed = orderRepository.countByStatus("CONFIRMED");
        long failed = orderRepository.countByStatus("FAILED_INVENTORY");
        long fraudSuspected = orderRepository.countByStatus("FRAUD_SUSPECTED");

        long pendingOutbox = outboxEventRepository.countByStatus("PENDING");
        long sentOutbox = outboxEventRepository.countByStatus("SENT");
        long failedOutbox = outboxEventRepository.countByStatus("FAILED_MAX_RETRIES");

        long dltCount = deadLetterEventRepository.count();
        long processedCount = processedEventRepository.count();

        List<OrderEntity> recentOrders = orderRepository.findTop50ByOrderByCreatedAtDesc();
        double totalRev = recentOrders.stream()
                .filter(o -> "CONFIRMED".equalsIgnoreCase(o.getStatus()))
                .mapToDouble(OrderEntity::getAmount)
                .sum();

        dto.setTotalOrders(totalOrders);
        dto.setConfirmedOrders(confirmed);
        dto.setFailedOrders(failed);
        dto.setFraudSuspectedOrders(fraudSuspected);

        dto.setPendingOutboxEvents(pendingOutbox);
        dto.setSentOutboxEvents(sentOutbox);
        dto.setFailedOutboxEvents(failedOutbox);

        dto.setDeadLetterQueueCount(dltCount);
        dto.setIdempotentEventsProcessed(processedCount);
        dto.setTotalRevenue(totalRev);

        dto.setRecentOrders(recentOrders);
        dto.setRecentFraudAlerts(fraudAlertsListener.getRecentAlerts());
        dto.setRecentDeadLetters(deadLetterQueueService.getRecentDeadLetters());
        dto.setRecentOutboxEvents(outboxEventRepository.findTop50ByOrderByCreatedAtDesc());
        dto.setTotalFraudAlerts(dto.getRecentFraudAlerts().size());

        return ResponseEntity.ok(dto);
    }

    /**
     * 10. Tương thích ngược với curl trong README cũ
     */
    @GetMapping("/system-status")
    @Operation(summary = "Trạng thái tổng quan hệ thống")
    public ResponseEntity<Map<String, Object>> getSystemStatus() {
        Map<String, Object> res = new HashMap<>();
        res.put("outboxPendingCount", outboxEventRepository.countByStatus("PENDING"));
        res.put("outboxSentCount", outboxEventRepository.countByStatus("SENT"));
        res.put("outboxFailedMaxRetriesCount", outboxEventRepository.countByStatus("FAILED_MAX_RETRIES"));
        res.put("idempotentProcessedEventsCount", processedEventRepository.count());
        res.put("deadLetterEventsCount", deadLetterEventRepository.count());
        res.put("activeFraudAlertsCount", fraudAlertsListener.getRecentAlerts().size());
        res.put("dashboardUrl", "http://localhost:8080");
        res.put("swaggerUrl", "http://localhost:8080/swagger-ui.html");
        res.put("h2ConsoleUrl", "http://localhost:8080/h2-console");
        return ResponseEntity.ok(res);
    }
}
