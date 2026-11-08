package com.ecommerce.payment.controller;

import com.ecommerce.payment.entity.PaymentEntity;
import com.ecommerce.payment.repository.PaymentRepository;
import com.ecommerce.payment.repository.ProcessedEventRepository;
import com.ecommerce.payment.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/payments")
@Tag(name = "Payment Management API", description = "Quản lý thanh toán và giao dịch")
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;

    public PaymentController(PaymentService paymentService,
                             PaymentRepository paymentRepository,
                             ProcessedEventRepository processedEventRepository) {
        this.paymentService = paymentService;
        this.paymentRepository = paymentRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @GetMapping
    @Operation(summary = "Xem lịch sử giao dịch thanh toán")
    public ResponseEntity<List<PaymentEntity>> getPayments() {
        return ResponseEntity.ok(paymentService.getRecentPayments());
    }

    @GetMapping("/metrics")
    @Operation(summary = "Thống kê Payment Service")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalTransactions", paymentRepository.count());
        metrics.put("successTransactions", paymentRepository.countByStatus("SUCCESS"));
        metrics.put("failedTransactions", paymentRepository.countByStatus("FAILED"));
        metrics.put("idempotentEventsProcessed", processedEventRepository.count());
        return ResponseEntity.ok(metrics);
    }
}

