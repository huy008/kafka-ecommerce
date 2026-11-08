package com.ecommerce.notification.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

public class NotificationLog implements Serializable {

    private String id;
    private String type; // ORDER_CONFIRMED, ORDER_FAILED, FRAUD_WARNING
    private String recipient;
    private String title;
    private String content;
    private String timestamp;

    public NotificationLog() {
        this.id = "NOTIF-" + UUID.randomUUID().toString().substring(0, 8);
        this.timestamp = LocalDateTime.now().toString();
    }

    public NotificationLog(String type, String recipient, String title, String content) {
        this.id = "NOTIF-" + UUID.randomUUID().toString().substring(0, 8);
        this.type = type;
        this.recipient = recipient;
        this.title = title;
        this.content = content;
        this.timestamp = LocalDateTime.now().toString();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getRecipient() { return recipient; }
    public void setRecipient(String recipient) { this.recipient = recipient; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
}

