package com.ecommerce.common.event;

import java.io.Serializable;
import java.time.LocalDateTime;

public class InventoryReleaseCommand implements Serializable {

    private String eventId;
    private String orderNumber;
    private String productName;
    private Integer quantity;
    private String reason;
    private String timestamp;

    public InventoryReleaseCommand() {
        this.timestamp = LocalDateTime.now().toString();
    }

    public InventoryReleaseCommand(String eventId, String orderNumber, String productName, Integer quantity, String reason) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.productName = productName;
        this.quantity = quantity;
        this.reason = reason;
        this.timestamp = LocalDateTime.now().toString();
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getOrderNumber() { return orderNumber; }
    public void setOrderNumber(String orderNumber) { this.orderNumber = orderNumber; }

    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }

    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
}

