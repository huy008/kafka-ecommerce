package com.ecommerce.voucher.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "vouchers")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VoucherEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String code;

    private String description;

    // PERCENTAGE (discount by percentage) or FIXED_AMOUNT (fixed amount discount)
    @Column(nullable = false)
    private String discountType;

    @Column(nullable = false)
    private Double discountValue;

    private Double maxDiscountAmount;

    @Builder.Default
    private Double minOrderAmount = 0.0;

    @Builder.Default
    private Integer usageLimit = 100;

    @Builder.Default
    private Integer usedCount = 0;

    // Status: ACTIVE, EXHAUSTED, INACTIVE
    @Column(nullable = false)
    @Builder.Default
    private String status = "ACTIVE";

    @Builder.Default
    private LocalDateTime validFrom = LocalDateTime.now().minusDays(1);

    @Builder.Default
    private LocalDateTime validTo = LocalDateTime.now().plusMonths(3);

    public VoucherEntity(String code, String description, String discountType, Double discountValue,
                         Double maxDiscountAmount, Double minOrderAmount, Integer usageLimit) {
        this.code = code;
        this.description = description;
        this.discountType = discountType;
        this.discountValue = discountValue;
        this.maxDiscountAmount = maxDiscountAmount;
        this.minOrderAmount = minOrderAmount != null ? minOrderAmount : 0.0;
        this.usageLimit = usageLimit != null ? usageLimit : 100;
        this.usedCount = 0;
        this.status = "ACTIVE";
        this.validFrom = LocalDateTime.now().minusDays(1);
        this.validTo = LocalDateTime.now().plusMonths(3);
    }

    /**
     * Calculates the actual discount amount for a given order amount.
     */
    public double calculateDiscount(double orderAmount) {
        double discount = 0.0;
        if ("PERCENTAGE".equalsIgnoreCase(discountType)) {
            discount = orderAmount * (discountValue / 100.0);
            if (maxDiscountAmount != null && discount > maxDiscountAmount) {
                discount = maxDiscountAmount;
            }
        } else {
            discount = discountValue;
        }
        return Math.min(discount, orderAmount);
    }

    /**
     * Validates whether the voucher is eligible for use.
     */
    public boolean canUse(double orderAmount) {
        if (!"ACTIVE".equalsIgnoreCase(status)) return false;
        if (minOrderAmount != null && orderAmount < minOrderAmount) return false;
        if (usageLimit != null && usedCount >= usageLimit) return false;
        LocalDateTime now = LocalDateTime.now();
        if (validFrom != null && now.isBefore(validFrom)) return false;
        if (validTo != null && now.isAfter(validTo)) return false;
        return true;
    }

    public synchronized boolean incrementUsage() {
        if (usageLimit != null && usedCount >= usageLimit) {
            this.status = "EXHAUSTED";
            return false;
        }
        this.usedCount = (this.usedCount == null ? 0 : this.usedCount) + 1;
        if (usageLimit != null && this.usedCount >= this.usageLimit) {
            this.status = "EXHAUSTED";
        }
        return true;
    }

    public synchronized void decrementUsage() {
        if (this.usedCount != null && this.usedCount > 0) {
            this.usedCount--;
            if ("EXHAUSTED".equalsIgnoreCase(this.status) && this.usedCount < this.usageLimit) {
                this.status = "ACTIVE";
            }
        }
    }
}
