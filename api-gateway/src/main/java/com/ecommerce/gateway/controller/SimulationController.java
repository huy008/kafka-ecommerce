package com.ecommerce.gateway.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@RestController
@RequestMapping("/api/gateway/simulation")
@Tag(name = "Saga Simulation API", description = "Các kịch bản thực chiến mô phỏng Event-Driven Saga Choreography")
public class SimulationController {

    private final RestTemplate restTemplate;

    @Value("${services.order-service}")
    private String orderServiceUrl;

    public SimulationController(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * KỊCH BẢN 1 (HAPPY PATH): Đặt hàng thành công trọn vẹn Saga
     * Order -> Inventory Reserved -> Payment Success -> Order Confirmed -> Email/SMS Notif
     */
    @PostMapping("/happy-path")
    @Operation(summary = "Kịch bản 1 (Happy Path): Đặt hàng thành công trọn vẹn Saga")
    public ResponseEntity<Map<String, Object>> simulateHappyPath() {
        Map<String, Object> req = new HashMap<>();
        req.put("customerId", "VIP_CUSTOMER_" + (int)(Math.random() * 900 + 100));
        req.put("productName", "MacBook Pro M3 Max");
        req.put("quantity", 1);
        req.put("amount", 65000000.0);
        req.put("paymentMethod", "CREDIT_CARD");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(req, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(orderServiceUrl + "/api/orders", entity, Map.class);

        Map<String, Object> res = new HashMap<>();
        res.put("status", "SAGA_TRIGGERED");
        res.put("scenario", "1. Happy Path Saga");
        res.put("flow", "OrderService (PENDING) -> InventoryService (RESERVED) -> PaymentService (SUCCESS) -> OrderService (CONFIRMED) -> NotificationService (SMS/Email)");
        res.put("orderResponse", response.getBody());
        return ResponseEntity.ok(res);
    }

    /**
     * KỊCH BẢN 2 (INVENTORY FAILURE): Sản phẩm hết hàng trong kho
     * Order -> Inventory Fails -> Order FAILED_OUT_OF_STOCK -> Không gọi thanh toán
     */
    @PostMapping("/out-of-stock")
    @Operation(summary = "Kịch bản 2: Kho hết hàng (Inventory Failure)")
    public ResponseEntity<Map<String, Object>> simulateOutOfStock() {
        Map<String, Object> req = new HashMap<>();
        req.put("customerId", "USER_STOCK_CHECK");
        req.put("productName", "FAIL_INVENTORY_PRODUCT");
        req.put("quantity", 1);
        req.put("amount", 25000000.0);
        req.put("paymentMethod", "CREDIT_CARD");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(req, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(orderServiceUrl + "/api/orders", entity, Map.class);

        Map<String, Object> res = new HashMap<>();
        res.put("status", "SAGA_TRIGGERED");
        res.put("scenario", "2. Inventory Failure");
        res.put("flow", "OrderService (PENDING) -> InventoryService (OUT_OF_STOCK) -> OrderService (FAILED_OUT_OF_STOCK) -> NotificationService (Thông báo hết hàng)");
        res.put("orderResponse", response.getBody());
        return ResponseEntity.ok(res);
    }

    /**
     * KỊCH BẢN 3 (SAGA COMPENSATING ROLLBACK): Thanh toán thất bại -> Tự động xả kho bù trừ
     * Order -> Inventory Reserved -> Payment Fails -> Lệnh Bù Trừ (Release Stock) -> Order FAILED_PAYMENT
     */
    @PostMapping("/payment-failure")
    @Operation(summary = "Kịch bản 3 (Saga Rollback): Thanh toán lỗi -> Tự động xả kho bù trừ")
    public ResponseEntity<Map<String, Object>> simulatePaymentFailure() {
        Map<String, Object> req = new HashMap<>();
        req.put("customerId", "FAIL_PAYMENT_USER");
        req.put("productName", "iPhone 16 Pro Max");
        req.put("quantity", 1);
        req.put("amount", 34000000.0);
        req.put("paymentMethod", "CREDIT_CARD");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(req, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(orderServiceUrl + "/api/orders", entity, Map.class);

        Map<String, Object> res = new HashMap<>();
        res.put("status", "SAGA_TRIGGERED");
        res.put("scenario", "3. Saga Compensating Rollback");
        res.put("flow", "Order (PENDING) -> Kho giữ hàng -> Thanh toán bị từ chối -> Kích hoạt lệnh BÙ TRỪ: Kho xả hàng hoàn lại tồn kho -> Order (FAILED_PAYMENT)");
        res.put("orderResponse", response.getBody());
        return ResponseEntity.ok(res);
    }

    /**
     * KỊCH BẢN 4 (REAL-TIME FRAUD DETECTION): Hacker đặt dồn dập 4 đơn / 1 giây
     * 4 đơn -> Kafka Streams Window 30s -> Fraud Alert -> Đơn hàng bị khóa FRAUD_SUSPECTED
     */
    @PostMapping("/fraud-attack")
    @Operation(summary = "Kịch bản 4: Hacker tấn công SPAM đơn hàng -> Kafka Streams phát hiện")
    public ResponseEntity<Map<String, Object>> simulateFraudAttack(@RequestParam(defaultValue = "hacker_99") String customerId) {
        List<Map> createdOrders = new ArrayList<>();

        for (int i = 1; i <= 4; i++) {
            Map<String, Object> req = new HashMap<>();
            req.put("customerId", customerId);
            req.put("productName", "PlayStation 5 Pro");
            req.put("quantity", 1);
            req.put("amount", 16500000.0);
            req.put("paymentMethod", "CREDIT_CARD");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(req, headers);

            try {
                ResponseEntity<Map> response = restTemplate.postForEntity(orderServiceUrl + "/api/orders", entity, Map.class);
                createdOrders.add(response.getBody());
            } catch (Exception ignored) {}
        }

        Map<String, Object> res = new HashMap<>();
        res.put("status", "ATTACK_SIMULATED");
        res.put("scenario", "4. Kafka Streams Real-time Fraud Detection");
        res.put("targetUser", customerId);
        res.put("ordersSpawned", createdOrders);
        res.put("flow", "4 đơn phát sinh trong 1s -> Kafka Streams Sliding Window 30s gom nhóm -> Phát sinh FraudAlertEvent -> Order Service tự động khóa cảnh báo FRAUD_SUSPECTED!");
        return ResponseEntity.ok(res);
    }
}

