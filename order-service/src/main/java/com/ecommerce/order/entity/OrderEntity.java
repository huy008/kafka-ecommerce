package com.ecommerce.order.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String orderNumber;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String productName;

    @Builder.Default
    private Integer quantity = 1;

    private Double unitPrice;

    @Column(nullable = false)
    private Double amount;

    @Builder.Default
    private String paymentMethod = "CREDIT_CARD";

    @Builder.Default
    private String shippingAddress = "Vietnam";

    // PENDING, INVENTORY_RESERVED, CONFIRMED, FAILED_OUT_OF_STOCK, FAILED_PAYMENT, FRAUD_SUSPECTED
    @Column(nullable = false)
    private String status;

    private String failureReason;

    @Builder.Default
    private Boolean fraudSuspected = false;

    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();

    @Builder.Default
    private LocalDateTime updatedAt = LocalDateTime.now();

    public OrderEntity(String orderNumber, String customerId, String productName, Integer quantity,
                       Double unitPrice, Double amount, String paymentMethod, String shippingAddress, String status) {
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.productName = productName;
        this.quantity = quantity != null ? quantity : 1;
        this.unitPrice = unitPrice;
        this.amount = amount;
        this.paymentMethod = paymentMethod != null ? paymentMethod : "CREDIT_CARD";
        this.shippingAddress = shippingAddress != null ? shippingAddress : "Vietnam";
        this.status = status;
        this.fraudSuspected = false;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }
}
