package com.ecommerce.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentFailedEvent implements Serializable {

    private String eventId;
    private String orderNumber;
    private String customerId;
    private String productName;
    private Integer quantity;
    private Double amount;
    private String reason;
    @Builder.Default
    private String timestamp = LocalDateTime.now().toString();

    public PaymentFailedEvent(String eventId, String orderNumber, String customerId, String productName,
                              Integer quantity, Double amount, String reason) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.productName = productName;
        this.quantity = quantity;
        this.amount = amount;
        this.reason = reason;
        this.timestamp = LocalDateTime.now().toString();
    }
}
