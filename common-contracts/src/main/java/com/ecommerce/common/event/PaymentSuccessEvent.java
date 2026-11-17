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
public class PaymentSuccessEvent implements Serializable {

    private String eventId;
    private String paymentId;
    private String orderNumber;
    private String customerId;
    private Double amount;
    private String paymentMethod;
    @Builder.Default
    private String timestamp = LocalDateTime.now().toString();

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
}
