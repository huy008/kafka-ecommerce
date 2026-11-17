package com.ecommerce.gateway.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/gateway")
@RequiredArgsConstructor
@Tag(name = "API Gateway Controller", description = "Reverse routing and metrics aggregation for microservices cluster")
public class GatewayRoutingController {

    private final RestTemplate restTemplate;

    @Value("${services.order-service}")
    private String orderServiceUrl;

    @Value("${services.inventory-service}")
    private String inventoryServiceUrl;

    @Value("${services.payment-service}")
    private String paymentServiceUrl;

    @Value("${services.fraud-detection-service}")
    private String fraudServiceUrl;

    @Value("${services.notification-service}")
    private String notificationServiceUrl;

    @Value("${services.voucher-service}")
    private String voucherServiceUrl;

    /**
     * Aggregates real-time metrics and state from all microservices for Mission Control Dashboard.
     */
    @GetMapping("/dashboard-data")
    @Operation(summary = "Fetch real-time metrics and status from all microservices")
    public ResponseEntity<Map<String, Object>> getDashboardData() {
        Map<String, Object> aggregated = new HashMap<>();

        // 1. Fetch data from Order Service
        try {
            Map orderMetrics = restTemplate.getForObject(orderServiceUrl + "/api/orders/metrics", Map.class);
            List ordersList = restTemplate.getForObject(orderServiceUrl + "/api/orders", List.class);
            aggregated.put("orderMetrics", orderMetrics);
            aggregated.put("orders", ordersList != null ? ordersList : Collections.emptyList());
            aggregated.put("orderServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("orderServiceOnline", false);
            aggregated.put("orders", Collections.emptyList());
        }

        // 2. Fetch data from Inventory Service
        try {
            List stocks = restTemplate.getForObject(inventoryServiceUrl + "/api/inventory/stocks", List.class);
            Map invMetrics = restTemplate.getForObject(inventoryServiceUrl + "/api/inventory/metrics", Map.class);
            aggregated.put("stocks", stocks != null ? stocks : Collections.emptyList());
            aggregated.put("inventoryMetrics", invMetrics);
            aggregated.put("inventoryServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("inventoryServiceOnline", false);
            aggregated.put("stocks", Collections.emptyList());
        }

        // 3. Fetch data from Payment Service
        try {
            List payments = restTemplate.getForObject(paymentServiceUrl + "/api/payments", List.class);
            Map payMetrics = restTemplate.getForObject(paymentServiceUrl + "/api/payments/metrics", Map.class);
            aggregated.put("payments", payments != null ? payments : Collections.emptyList());
            aggregated.put("paymentMetrics", payMetrics);
            aggregated.put("paymentServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("paymentServiceOnline", false);
            aggregated.put("payments", Collections.emptyList());
        }

        // 4. Fetch data from Fraud Detection Service
        try {
            List alerts = restTemplate.getForObject(fraudServiceUrl + "/api/fraud/alerts", List.class);
            aggregated.put("fraudAlerts", alerts != null ? alerts : Collections.emptyList());
            aggregated.put("fraudServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("fraudServiceOnline", false);
            aggregated.put("fraudAlerts", Collections.emptyList());
        }

        // 5. Fetch data from Notification Service
        try {
            List notifs = restTemplate.getForObject(notificationServiceUrl + "/api/notifications", List.class);
            aggregated.put("notifications", notifs != null ? notifs : Collections.emptyList());
            aggregated.put("notificationServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("notificationServiceOnline", false);
            aggregated.put("notifications", Collections.emptyList());
        }

        // 6. Fetch data from Voucher Service
        try {
            List vouchers = restTemplate.getForObject(voucherServiceUrl + "/api/vouchers", List.class);
            Map voucherMetrics = restTemplate.getForObject(voucherServiceUrl + "/api/vouchers/metrics", Map.class);
            aggregated.put("vouchers", vouchers != null ? vouchers : Collections.emptyList());
            aggregated.put("voucherMetrics", voucherMetrics);
            aggregated.put("voucherServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("voucherServiceOnline", false);
            aggregated.put("vouchers", Collections.emptyList());
        }

        return ResponseEntity.ok(aggregated);
    }
}
