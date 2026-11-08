package com.ecommerce.gateway.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@RestController
@RequestMapping("/api/gateway")
@Tag(name = "API Gateway Controller", description = "Định tuyến và tổng hợp dữ liệu từ cụm Microservices")
public class GatewayRoutingController {

    private static final Logger log = LoggerFactory.getLogger(GatewayRoutingController.class);

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

    public GatewayRoutingController(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * TỔNG HỢP TOÀN BỘ METRICS TỪ CỤM MICROSERVICES CHO MISSION CONTROL DASHBOARD
     */
    @GetMapping("/dashboard-data")
    @Operation(summary = "Lấy dữ liệu thời gian thực từ tất cả các microservices")
    public ResponseEntity<Map<String, Object>> getDashboardData() {
        Map<String, Object> aggregated = new HashMap<>();

        // 1. Lấy dữ liệu từ Order Service
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

        // 2. Lấy dữ liệu từ Inventory Service
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

        // 3. Lấy dữ liệu từ Payment Service
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

        // 4. Lấy dữ liệu từ Fraud Detection Service
        try {
            List alerts = restTemplate.getForObject(fraudServiceUrl + "/api/fraud/alerts", List.class);
            aggregated.put("fraudAlerts", alerts != null ? alerts : Collections.emptyList());
            aggregated.put("fraudServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("fraudServiceOnline", false);
            aggregated.put("fraudAlerts", Collections.emptyList());
        }

        // 5. Lấy dữ liệu từ Notification Service
        try {
            List notifs = restTemplate.getForObject(notificationServiceUrl + "/api/notifications", List.class);
            aggregated.put("notifications", notifs != null ? notifs : Collections.emptyList());
            aggregated.put("notificationServiceOnline", true);
        } catch (Exception e) {
            aggregated.put("notificationServiceOnline", false);
            aggregated.put("notifications", Collections.emptyList());
        }

        return ResponseEntity.ok(aggregated);
    }
}

