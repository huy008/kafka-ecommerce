package com.ecommerce.voucher.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "voucher_usages")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VoucherUsageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String voucherCode;

    @Column(nullable = false)
    private String orderNumber;

    private String customerId;

    private Double orderAmount;

    private Double discountAmount;

    private Double finalAmount;

    // Status: APPLIED, RELEASED
    @Column(nullable = false)
    private String status;

    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();

    @Builder.Default
    private LocalDateTime updatedAt = LocalDateTime.now();

    public VoucherUsageEntity(String voucherCode, String orderNumber, String customerId,
                              Double orderAmount, Double discountAmount, Double finalAmount, String status) {
        this.voucherCode = voucherCode;
        this.orderNumber = orderNumber;
        this.customerId = customerId;
        this.orderAmount = orderAmount;
        this.discountAmount = discountAmount;
        this.finalAmount = finalAmount;
        this.status = status;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }
}
