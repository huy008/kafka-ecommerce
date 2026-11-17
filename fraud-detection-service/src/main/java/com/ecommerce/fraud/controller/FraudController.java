package com.ecommerce.fraud.controller;

import com.ecommerce.common.event.FraudAlertEvent;
import com.ecommerce.fraud.service.FraudAlertStore;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/fraud")
@RequiredArgsConstructor
@Tag(name = "Fraud Detection API", description = "Real-time fraud alerts from Kafka Streams Engine")
public class FraudController {

    private final FraudAlertStore alertStore;

    @GetMapping("/alerts")
    @Operation(summary = "Get recent fraud alerts")
    public ResponseEntity<List<FraudAlertEvent>> getAlerts() {
        return ResponseEntity.ok(alertStore.getRecentAlerts());
    }

    @DeleteMapping("/alerts")
    @Operation(summary = "Clear fraud alert history")
    public ResponseEntity<Map<String, String>> clearAlerts() {
        alertStore.clearAlerts();
        return ResponseEntity.ok(Collections.singletonMap("message", "Fraud alert history cleared successfully."));
    }

    @GetMapping("/metrics")
    @Operation(summary = "Get Fraud Detection Service metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("activeAlertsCount", alertStore.getRecentAlerts().size());
        return ResponseEntity.ok(metrics);
    }
}
