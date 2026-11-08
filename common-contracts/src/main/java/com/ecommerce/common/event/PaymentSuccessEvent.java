package com.ecommerce.common.event;

import java.io.Serializable;
import java.time.LocalDateTime;

public class PaymentSuccessEvent implements Serializable {

    private String eventId;
    private String paymentId;
    private String orderNumber;
    private String customerId;
    private Double amount;
    private String paymentMethod;
    private String timestamp;

    public PaymentSuccessEvent() {
        this.timestamp = LocalDateTime.now().toString();
    }

    public PaymentSuccessEvent(String eventId, String paymentId, String orderNumber, String customerId,
                               Double amount, String paymentMethod) {
        this.eventId = eventId;
        this.paymentId = paymentId;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.amount = amount;
        this.paymentMethod = paymentMethod;
        this.timestamp = LocalDateTime.now().toString();
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getPaymentId() { return paymentId; }
    public void setPaymentId(String paymentId) { this.paymentId = paymentId; }

    public String getOrderNumber() { return orderNumber; }
    public void setOrderNumber(String orderNumber) { this.orderNumber = orderNumber; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public Double getAmount() { return amount; }
    public void setAmount(Double amount) { this.amount = amount; }

    public String getPaymentMethod() { return paymentMethod; }
    public void setPaymentMethod(String paymentMethod) { this.paymentMethod = paymentMethod; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
}

