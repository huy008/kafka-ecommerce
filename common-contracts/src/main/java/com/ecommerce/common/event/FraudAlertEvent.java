package com.ecommerce.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FraudAlertEvent implements Serializable {

    @Builder.Default
    private String alertId = "ALT-" + UUID.randomUUID().toString().substring(0, 8);
    private String customerId;
    private long orderCountInWindow;
    private double totalAmountInWindow;
    private String alertReason;
    @Builder.Default
    private String timestamp = LocalDateTime.now().toString();

    public FraudAlertEvent(String customerId, long orderCountInWindow, double totalAmountInWindow, String alertReason) {
        this.alertId = "ALT-" + UUID.randomUUID().toString().substring(0, 8);
        this.customerId = customerId;
        this.orderCountInWindow = orderCountInWindow;
        this.totalAmountInWindow = totalAmountInWindow;
        this.alertReason = alertReason;
        this.timestamp = LocalDateTime.now().toString();
    }
}
