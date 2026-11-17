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
public class InventoryReservedEvent implements Serializable {

    private String eventId;
    private String orderNumber;
    private String customerId;
    private String productName;
    private Integer quantity;
    private Double totalAmount;
    private String paymentMethod;
    @Builder.Default
    private String timestamp = LocalDateTime.now().toString();

    public InventoryReservedEvent(String eventId, String orderNumber, String customerId, String productName,
                                  Integer quantity, Double totalAmount, String paymentMethod) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.productName = productName;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.paymentMethod = paymentMethod;
        this.timestamp = LocalDateTime.now().toString();
    }
}
