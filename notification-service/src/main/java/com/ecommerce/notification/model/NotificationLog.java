package com.ecommerce.notification.model;

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
public class NotificationLog implements Serializable {

    @Builder.Default
    private String id = "NOTIF-" + UUID.randomUUID().toString().substring(0, 8);
    // Types: ORDER_CONFIRMED, ORDER_FAILED, FRAUD_WARNING, OUT_OF_STOCK, SECURITY_ALERT
    private String type;
    private String recipient;
    private String title;
    private String content;
    @Builder.Default
    private String timestamp = LocalDateTime.now().toString();

    public NotificationLog(String type, String recipient, String title, String content) {
        this.id = "NOTIF-" + UUID.randomUUID().toString().substring(0, 8);
        this.type = type;
        this.recipient = recipient;
        this.title = title;
        this.content = content;
        this.timestamp = LocalDateTime.now().toString();
    }
}
