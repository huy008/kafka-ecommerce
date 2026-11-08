package com.ecommerce.fraud.controller;

import com.ecommerce.common.event.FraudAlertEvent;
import com.ecommerce.fraud.service.FraudAlertStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/fraud")
@Tag(name = "Fraud Detection API", description = "Cảnh báo gian lận thời gian thực từ Kafka Streams Engine")
public class FraudController {

    private final FraudAlertStore alertStore;

    public FraudController(FraudAlertStore alertStore) {
        this.alertStore = alertStore;
    }

    @GetMapping("/alerts")
    @Operation(summary = "Xem danh sách cảnh báo gian lận gần nhất")
    public ResponseEntity<List<FraudAlertEvent>> getAlerts() {
        return ResponseEntity.ok(alertStore.getRecentAlerts());
    }

    @DeleteMapping("/alerts")
    @Operation(summary = "Xóa lịch sử cảnh báo gian lận")
    public ResponseEntity<Map<String, String>> clearAlerts() {
        alertStore.clearAlerts();
        return ResponseEntity.ok(Collections.singletonMap("message", "Đã xóa sạch lịch sử cảnh báo gian lận."));
    }

    @GetMapping("/metrics")
    @Operation(summary = "Thống kê Fraud Detection Service")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("activeAlertsCount", alertStore.getRecentAlerts().size());
        return ResponseEntity.ok(metrics);
    }
}

