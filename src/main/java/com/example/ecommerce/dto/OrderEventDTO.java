package com.example.ecommerce.dto;

import java.io.Serializable;
import java.time.LocalDateTime;

public class OrderEventDTO implements Serializable {

    private String eventId;
    private String orderNumber;
    private String customerId;
    private String productName;
    private Integer quantity;
    private Double unitPrice;
    private Double amount;
    private String paymentMethod;
    private String shippingAddress;
    private String timestamp;

    public OrderEventDTO() {
        this.timestamp = LocalDateTime.now().toString();
        this.quantity = 1;
    }

    public OrderEventDTO(String eventId, String orderNumber, String customerId, String productName, Double amount) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.productName = productName;
        this.amount = amount;
        this.unitPrice = amount;
        this.quantity = 1;
        this.paymentMethod = "CREDIT_CARD";
        this.shippingAddress = "Vietnam";
        this.timestamp = LocalDateTime.now().toString();
    }

    public OrderEventDTO(String eventId, String orderNumber, String customerId, String productName,
                         Integer quantity, Double unitPrice, Double amount, String paymentMethod, String shippingAddress) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.productName = productName;
        this.quantity = quantity != null ? quantity : 1;
        this.unitPrice = unitPrice;
        this.amount = amount;
        this.paymentMethod = paymentMethod;
        this.shippingAddress = shippingAddress;
        this.timestamp = LocalDateTime.now().toString();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(Double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getShippingAddress() {
        return shippingAddress;
    }

    public void setShippingAddress(String shippingAddress) {
        this.shippingAddress = shippingAddress;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEventDTO{" +
                "eventId='" + eventId + '\'' +
                ", orderNumber='" + orderNumber + '\'' +
                ", customerId='" + customerId + '\'' +
                ", productName='" + productName + '\'' +
                ", quantity=" + quantity +
                ", amount=" + amount +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
