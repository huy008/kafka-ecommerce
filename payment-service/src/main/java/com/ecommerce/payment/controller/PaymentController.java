package com.ecommerce.payment.controller;

import com.ecommerce.payment.entity.PaymentEntity;
import com.ecommerce.payment.repository.PaymentRepository;
import com.ecommerce.payment.repository.ProcessedEventRepository;
import com.ecommerce.payment.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Tag(name = "Payment Management API", description = "Payment transactions and idempotency management")
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;

    @GetMapping
    @Operation(summary = "Get recent payment transactions")
    public ResponseEntity<List<PaymentEntity>> getPayments() {
        return ResponseEntity.ok(paymentService.getRecentPayments());
    }

    @GetMapping("/metrics")
    @Operation(summary = "Get Payment Service metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalTransactions", paymentRepository.count());
        metrics.put("successTransactions", paymentRepository.countByStatus("SUCCESS"));
        metrics.put("failedTransactions", paymentRepository.countByStatus("FAILED"));
        metrics.put("idempotentEventsProcessed", processedEventRepository.count());
        return ResponseEntity.ok(metrics);
    }
}
