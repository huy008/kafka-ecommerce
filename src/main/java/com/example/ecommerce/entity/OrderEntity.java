package com.example.ecommerce.entity;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String orderNumber;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String productName;

    private Integer quantity;

    private Double unitPrice;

    @Column(nullable = false)
    private Double amount;

    private String paymentMethod;

    private String shippingAddress;

    @Column(nullable = false)
    private String status; // PENDING, CONFIRMED, FAILED_INVENTORY, FRAUD_SUSPECTED, CANCELLED

    private String failureReason;

    private Boolean fraudSuspected = false;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public OrderEntity() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
        this.quantity = 1;
        this.fraudSuspected = false;
    }

    public OrderEntity(String orderNumber, String customerId, String productName, Integer quantity,
                       Double unitPrice, Double amount, String paymentMethod, String shippingAddress, String status) {
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.productName = productName;
        this.quantity = quantity != null ? quantity : 1;
        this.unitPrice = unitPrice;
        this.amount = amount;
        this.paymentMethod = paymentMethod != null ? paymentMethod : "CREDIT_CARD";
        this.shippingAddress = shippingAddress != null ? shippingAddress : "Vietnam";
        this.status = status;
        this.fraudSuspected = false;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    // Constructor tương thích ngược cho các hàm gọi cũ
    public OrderEntity(String orderNumber, String customerId, String productName, Double amount, String status) {
        this(orderNumber, customerId, productName, 1, amount, amount, "CREDIT_CARD", "Standard Shipping", status);
    }

    public Long getId() {
        return id;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        this.updatedAt = LocalDateTime.now();
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
        this.updatedAt = LocalDateTime.now();
    }

    public Boolean getFraudSuspected() {
        return fraudSuspected;
    }

    public void setFraudSuspected(Boolean fraudSuspected) {
        this.fraudSuspected = fraudSuspected;
        this.updatedAt = LocalDateTime.now();
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
