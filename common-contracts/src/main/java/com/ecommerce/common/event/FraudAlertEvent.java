package com.ecommerce.common.event;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

public class FraudAlertEvent implements Serializable {

    private String alertId;
    private String customerId;
    private long orderCountInWindow;
    private double totalAmountInWindow;
    private String alertReason;
    private String timestamp;

    public FraudAlertEvent() {
        this.alertId = "ALT-" + UUID.randomUUID().toString().substring(0, 8);
        this.timestamp = LocalDateTime.now().toString();
    }

    public FraudAlertEvent(String customerId, long orderCountInWindow, double totalAmountInWindow, String alertReason) {
        this.alertId = "ALT-" + UUID.randomUUID().toString().substring(0, 8);
        this.customerId = customerId;
        this.orderCountInWindow = orderCountInWindow;
        this.totalAmountInWindow = totalAmountInWindow;
        this.alertReason = alertReason;
        this.timestamp = LocalDateTime.now().toString();
    }

    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public long getOrderCountInWindow() { return orderCountInWindow; }
    public void setOrderCountInWindow(long orderCountInWindow) { this.orderCountInWindow = orderCountInWindow; }

    public double getTotalAmountInWindow() { return totalAmountInWindow; }
    public void setTotalAmountInWindow(double totalAmountInWindow) { this.totalAmountInWindow = totalAmountInWindow; }

    public String getAlertReason() { return alertReason; }
    public void setAlertReason(String alertReason) { this.alertReason = alertReason; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
}

