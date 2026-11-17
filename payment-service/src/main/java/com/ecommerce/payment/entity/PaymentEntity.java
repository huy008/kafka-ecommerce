package com.ecommerce.payment.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "payments")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String paymentId;

    @Column(nullable = false)
    private String orderNumber;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private Double amount;

    private String paymentMethod;

    // Status: SUCCESS, FAILED
    @Column(nullable = false)
    private String status;

    private String failureReason;

    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();

    public PaymentEntity(String paymentId, String orderNumber, String customerId, Double amount,
                         String paymentMethod, String status, String failureReason) {
        this.paymentId = paymentId;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.amount = amount;
        this.paymentMethod = paymentMethod != null ? paymentMethod : "CREDIT_CARD";
        this.status = status;
        this.failureReason = failureReason;
        this.createdAt = LocalDateTime.now();
    }
}
