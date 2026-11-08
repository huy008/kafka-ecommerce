package com.example.ecommerce.dto;

import java.io.Serializable;

public class CustomerActivitySummary implements Serializable {

    private String customerId;
    private long orderCount;
    private double totalAmount;

    public CustomerActivitySummary() {
        this.orderCount = 0;
        this.totalAmount = 0.0;
    }

    public CustomerActivitySummary(String customerId, long orderCount, double totalAmount) {
        this.customerId = customerId;
        this.orderCount = orderCount;
        this.totalAmount = totalAmount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public CustomerActivitySummary addOrder(double amount) {
        this.orderCount++;
        this.totalAmount += amount;
        return this;
    }
}

