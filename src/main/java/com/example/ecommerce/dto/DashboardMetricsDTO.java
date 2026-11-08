package com.example.ecommerce.dto;

import com.example.ecommerce.entity.DeadLetterEventEntity;
import com.example.ecommerce.entity.OrderEntity;
import com.example.ecommerce.entity.OutboxEventEntity;

import java.io.Serializable;
import java.util.List;

public class DashboardMetricsDTO implements Serializable {

    private long totalOrders;
    private long confirmedOrders;
    private long failedOrders;
    private long fraudSuspectedOrders;
    private long pendingOutboxEvents;
    private long sentOutboxEvents;
    private long failedOutboxEvents;
    private long deadLetterQueueCount;
    private long totalFraudAlerts;
    private long idempotentEventsProcessed;
    private double totalRevenue;

    private List<OrderEntity> recentOrders;
    private List<FraudAlertEvent> recentFraudAlerts;
    private List<DeadLetterEventEntity> recentDeadLetters;
    private List<OutboxEventEntity> recentOutboxEvents;

    public DashboardMetricsDTO() {
    }

    public long getTotalOrders() {
        return totalOrders;
    }

    public void setTotalOrders(long totalOrders) {
        this.totalOrders = totalOrders;
    }

    public long getConfirmedOrders() {
        return confirmedOrders;
    }

    public void setConfirmedOrders(long confirmedOrders) {
        this.confirmedOrders = confirmedOrders;
    }

    public long getFailedOrders() {
        return failedOrders;
    }

    public void setFailedOrders(long failedOrders) {
        this.failedOrders = failedOrders;
    }

    public long getFraudSuspectedOrders() {
        return fraudSuspectedOrders;
    }

    public void setFraudSuspectedOrders(long fraudSuspectedOrders) {
        this.fraudSuspectedOrders = fraudSuspectedOrders;
    }

    public long getPendingOutboxEvents() {
        return pendingOutboxEvents;
    }

    public void setPendingOutboxEvents(long pendingOutboxEvents) {
        this.pendingOutboxEvents = pendingOutboxEvents;
    }

    public long getSentOutboxEvents() {
        return sentOutboxEvents;
    }

    public void setSentOutboxEvents(long sentOutboxEvents) {
        this.sentOutboxEvents = sentOutboxEvents;
    }

    public long getFailedOutboxEvents() {
        return failedOutboxEvents;
    }

    public void setFailedOutboxEvents(long failedOutboxEvents) {
        this.failedOutboxEvents = failedOutboxEvents;
    }

    public long getDeadLetterQueueCount() {
        return deadLetterQueueCount;
    }

    public void setDeadLetterQueueCount(long deadLetterQueueCount) {
        this.deadLetterQueueCount = deadLetterQueueCount;
    }

    public long getTotalFraudAlerts() {
        return totalFraudAlerts;
    }

    public void setTotalFraudAlerts(long totalFraudAlerts) {
        this.totalFraudAlerts = totalFraudAlerts;
    }

    public long getIdempotentEventsProcessed() {
        return idempotentEventsProcessed;
    }

    public void setIdempotentEventsProcessed(long idempotentEventsProcessed) {
        this.idempotentEventsProcessed = idempotentEventsProcessed;
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    public List<OrderEntity> getRecentOrders() {
        return recentOrders;
    }

    public void setRecentOrders(List<OrderEntity> recentOrders) {
        this.recentOrders = recentOrders;
    }

    public List<FraudAlertEvent> getRecentFraudAlerts() {
        return recentFraudAlerts;
    }

    public void setRecentFraudAlerts(List<FraudAlertEvent> recentFraudAlerts) {
        this.recentFraudAlerts = recentFraudAlerts;
    }

    public List<DeadLetterEventEntity> getRecentDeadLetters() {
        return recentDeadLetters;
    }

    public void setRecentDeadLetters(List<DeadLetterEventEntity> recentDeadLetters) {
        this.recentDeadLetters = recentDeadLetters;
    }

    public List<OutboxEventEntity> getRecentOutboxEvents() {
        return recentOutboxEvents;
    }

    public void setRecentOutboxEvents(List<OutboxEventEntity> recentOutboxEvents) {
        this.recentOutboxEvents = recentOutboxEvents;
    }
}

