package com.example.ecommerce.controller;

import com.example.ecommerce.dto.CreateOrderRequest;
import com.example.ecommerce.entity.OrderEntity;
import com.example.ecommerce.repository.OrderRepository;
import com.example.ecommerce.service.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@Tag(name = "1. Order Management API", description = "Quản lý đơn hàng thương mại điện tử với Transactional Outbox Pattern")
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;

    public OrderController(OrderService orderService, OrderRepository orderRepository) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
    }

    /**
     * API Tạo Đơn Hàng Chuẩn Doanh Nghiệp (Transactional Outbox):
     * Request trả về cực nhanh (sub-10ms) vì việc gửi Kafka được chuyển giao cho Outbox Publisher.
     */
    @PostMapping
    @Operation(summary = "Đặt đơn hàng mới", description = "Ghi đồng thời Order & Outbox Event vào cùng 1 SQL Transaction để chống Dual-Write.")
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
        response.put("paymentMethod", order.getPaymentMethod());
        response.put("orderStatus", order.getStatus());
        response.put("pattern", "Transactional Outbox Pattern - Đơn hàng đã ghi DB và Outbox Event sẵn sàng đẩy sang Kafka.");
        return ResponseEntity.ok(response);
    }

    @GetMapping
    @Operation(summary = "Lấy danh sách tất cả đơn hàng", description = "Truy xuất danh sách đơn hàng đã được ghi nhận trong cơ sở dữ liệu.")
    public ResponseEntity<List<OrderEntity>> getOrders() {
        return ResponseEntity.ok(orderRepository.findTop50ByOrderByCreatedAtDesc());
    }

    @GetMapping("/{orderNumber}")
    @Operation(summary = "Chi tiết đơn hàng theo mã đơn", description = "Kiểm tra trạng thái thời gian thực của đơn hàng (PENDING, CONFIRMED, FAILED_INVENTORY, FRAUD_SUSPECTED).")
    public ResponseEntity<?> getOrderByOrderNumber(@PathVariable String orderNumber) {
        return orderService.getOrderByOrderNumber(orderNumber)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
