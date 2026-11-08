package com.example.ecommerce.entity;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "dead_letter_events")
public class DeadLetterEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String eventId;

    private String orderNumber;

    private String topic;

    private Integer partitionNumber;

    private Long offsetValue;

    @Lob
    private String payload;

    @Lob
    private String errorMessage;

    @Column(nullable = false)
    private String status; // UNRESOLVED, REPLAYED

    private LocalDateTime receivedAt;

    private LocalDateTime replayedAt;

    public DeadLetterEventEntity() {
        this.receivedAt = LocalDateTime.now();
        this.status = "UNRESOLVED";
    }

    public DeadLetterEventEntity(String eventId, String orderNumber, String topic, Integer partitionNumber,
                                 Long offsetValue, String payload, String errorMessage) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.topic = topic;
        this.partitionNumber = partitionNumber;
        this.offsetValue = offsetValue;
        this.payload = payload;
        this.errorMessage = errorMessage;
        this.status = "UNRESOLVED";
        this.receivedAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(Integer partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    public Long getOffsetValue() {
        return offsetValue;
    }

    public void setOffsetValue(Long offsetValue) {
        this.offsetValue = offsetValue;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(LocalDateTime receivedAt) {
        this.receivedAt = receivedAt;
    }

    public LocalDateTime getReplayedAt() {
        return replayedAt;
    }

    public void setReplayedAt(LocalDateTime replayedAt) {
        this.replayedAt = replayedAt;
    }
}

