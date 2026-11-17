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
public class OrderCreatedEvent implements Serializable {

    private String eventId;
    private String orderNumber;
    private String customerId;
    private String productName;
    @Builder.Default
    private Integer quantity = 1;
    private Double unitPrice;
    private Double totalAmount;
    @Builder.Default
    private String paymentMethod = "CREDIT_CARD";
    @Builder.Default
    private String shippingAddress = "Vietnam";
    @Builder.Default
    private String timestamp = LocalDateTime.now().toString();

    public OrderCreatedEvent(String eventId, String orderNumber, String customerId, String productName,
                             Integer quantity, Double unitPrice, Double totalAmount, String paymentMethod, String shippingAddress) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.productName = productName;
        this.quantity = quantity != null ? quantity : 1;
        this.unitPrice = unitPrice;
        this.totalAmount = totalAmount;
        this.paymentMethod = paymentMethod != null ? paymentMethod : "CREDIT_CARD";
        this.shippingAddress = shippingAddress != null ? shippingAddress : "Vietnam";
        this.timestamp = LocalDateTime.now().toString();
    }
}
