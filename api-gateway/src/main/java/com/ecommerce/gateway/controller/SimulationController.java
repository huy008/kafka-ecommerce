package com.ecommerce.gateway.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
@Tag(name = "Saga Simulation API", description = "Real-world scenarios simulating Event-Driven Saga Choreography")
public class SimulationController {

    private final RestTemplate restTemplate;

    @Value("${services.order-service}")
    private String orderServiceUrl;

    /**
     * SCENARIO 1 (HAPPY PATH): Successful end-to-end Saga workflow.
     * Order -> Inventory Reserved -> Payment Success -> Order Confirmed -> Email/SMS Notif
     */
    @PostMapping("/happy-path")
    @Operation(summary = "Scenario 1 (Happy Path): Successful end-to-end Saga order placement")
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
     * SCENARIO 2 (INVENTORY FAILURE): Out of stock scenario.
     * Order -> Inventory Fails -> Order FAILED_OUT_OF_STOCK -> Payment not triggered
     */
    @PostMapping("/out-of-stock")
    @Operation(summary = "Scenario 2: Out of stock (Inventory Failure)")
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
        res.put("flow", "OrderService (PENDING) -> InventoryService (OUT_OF_STOCK) -> OrderService (FAILED_OUT_OF_STOCK) -> NotificationService (Out of stock notice)");
        res.put("orderResponse", response.getBody());
        return ResponseEntity.ok(res);
    }

    /**
     * SCENARIO 3 (SAGA COMPENSATING ROLLBACK): Payment failure triggers automatic warehouse compensation.
     * Order -> Inventory Reserved -> Payment Fails -> Compensation (Release Stock) -> Order FAILED_PAYMENT
     */
    @PostMapping("/payment-failure")
    @Operation(summary = "Scenario 3 (Saga Rollback): Payment failure -> Triggers compensating stock release")
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
        res.put("flow", "Order (PENDING) -> Inventory reserved -> Payment rejected -> COMPENSATING TRIGGER: Stock released back -> Order (FAILED_PAYMENT)");
        res.put("orderResponse", response.getBody());
        return ResponseEntity.ok(res);
    }

    /**
     * SCENARIO 4 (REAL-TIME FRAUD DETECTION): Rapid order placement within 1 second.
     * 4 orders -> Kafka Streams 30s Window -> Fraud Alert -> Order locked as FRAUD_SUSPECTED
     */
    @PostMapping("/fraud-attack")
    @Operation(summary = "Scenario 4: High-frequency order burst -> Kafka Streams real-time fraud alert")
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
        res.put("flow", "4 orders spawned in 1s -> Kafka Streams 30s sliding window aggregated -> FraudAlertEvent generated -> Order Service flags and locks as FRAUD_SUSPECTED!");
        return ResponseEntity.ok(res);
    }
}
