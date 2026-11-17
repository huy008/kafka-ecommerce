package com.ecommerce.order.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateOrderRequest {

    @NotBlank(message = "Customer ID must not be blank")
    private String customerId;

    @NotBlank(message = "Product name must not be blank")
    private String productName;

    @Min(value = 1, message = "Quantity must be at least 1")
    @Builder.Default
    private Integer quantity = 1;

    private Double unitPrice;

    @NotNull(message = "Amount must not be null")
    @Positive(message = "Amount must be greater than 0")
    private Double amount;

    @Builder.Default
    private String paymentMethod = "CREDIT_CARD";

    @Builder.Default
    private String shippingAddress = "Hanoi, Vietnam";

    public CreateOrderRequest(String customerId, String productName, Double amount) {
        this.customerId = customerId;
        this.productName = productName;
        this.amount = amount;
        this.unitPrice = amount;
        this.quantity = 1;
        this.paymentMethod = "CREDIT_CARD";
        this.shippingAddress = "Hanoi, Vietnam";
    }
}
