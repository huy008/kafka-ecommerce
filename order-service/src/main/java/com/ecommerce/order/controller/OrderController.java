package com.ecommerce.order.controller;

import com.ecommerce.order.dto.CreateOrderRequest;
import com.ecommerce.order.entity.OrderEntity;
import com.ecommerce.order.repository.OrderRepository;
import com.ecommerce.order.repository.OutboxEventRepository;
import com.ecommerce.order.service.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Tag(name = "Order Management API", description = "Order management with Transactional Outbox Pattern")
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;

    @PostMapping
    @Operation(summary = "Place a new order (Transactional Outbox)")
    public ResponseEntity<Map<String, Object>> createOrder(@Valid @RequestBody(required = false) CreateOrderRequest request) {
        if (request == null) {
            request = new CreateOrderRequest();
        }
        if (request.getCustomerId() == null || request.getCustomerId().trim().isEmpty()) {
            request.setCustomerId("CUST-" + (int)(Math.random() * 1000));
        }
        if (request.getProductName() == null || request.getProductName().trim().isEmpty()) {
            request.setProductName("Laptop Dell XPS 15");
        }
        if (request.getAmount() == null || request.getAmount() <= 0) {
            request.setAmount(25000000.0);
        }

        OrderEntity order = orderService.createOrder(request);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "ACCEPTED");
        response.put("orderNumber", order.getOrderNumber());
        response.put("customerId", order.getCustomerId());
        response.put("productName", order.getProductName());
        response.put("quantity", order.getQuantity());
        response.put("amount", order.getAmount());
        response.put("orderStatus", order.getStatus());
        response.put("message", "Order received successfully. Outbox event stored and waiting for Kafka publisher dispatch.");

        return ResponseEntity.accepted().body(response);
    }

    @GetMapping
    @Operation(summary = "Get list of recent orders")
    public ResponseEntity<List<OrderEntity>> getAllOrders() {
        return ResponseEntity.ok(orderService.getRecentOrders());
    }

    @GetMapping("/{orderNumber}")
    @Operation(summary = "Get order details by order number")
    public ResponseEntity<OrderEntity> getOrderByOrderNumber(@PathVariable String orderNumber) {
        return orderService.getOrderByOrderNumber(orderNumber)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/metrics")
    @Operation(summary = "Get Order Service statistics & Outbox status")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalOrders", orderRepository.count());
        metrics.put("confirmedOrders", orderRepository.countByStatus("CONFIRMED"));
        metrics.put("pendingOrders", orderRepository.countByStatus("PENDING"));
        metrics.put("failedOutOfStock", orderRepository.countByStatus("FAILED_OUT_OF_STOCK"));
        metrics.put("failedPayment", orderRepository.countByStatus("FAILED_PAYMENT"));
        metrics.put("fraudSuspected", orderRepository.countByStatus("FRAUD_SUSPECTED"));

        metrics.put("outboxPending", outboxEventRepository.countByStatus("PENDING"));
        metrics.put("outboxSent", outboxEventRepository.countByStatus("SENT"));
        metrics.put("outboxFailed", outboxEventRepository.countByStatus("FAILED_MAX_RETRIES"));

        return ResponseEntity.ok(metrics);
    }
}
