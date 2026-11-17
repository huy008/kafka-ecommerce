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
public class InventoryReleaseCommand implements Serializable {

    private String eventId;
    private String orderNumber;
    private String productName;
    private Integer quantity;
    private String reason;
    @Builder.Default
    private String timestamp = LocalDateTime.now().toString();

    public InventoryReleaseCommand(String eventId, String orderNumber, String productName, Integer quantity, String reason) {
        this.eventId = eventId;
        this.orderNumber = orderNumber;
        this.productName = productName;
        this.quantity = quantity;
        this.reason = reason;
        this.timestamp = LocalDateTime.now().toString();
    }
}
