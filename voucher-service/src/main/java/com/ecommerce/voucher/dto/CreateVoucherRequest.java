package com.ecommerce.voucher.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateVoucherRequest {
    private String code;
    private String description;
    // Discount types: PERCENTAGE or FIXED_AMOUNT
    private String discountType;
    private Double discountValue;
    private Double maxDiscountAmount;
    private Double minOrderAmount;
    private Integer usageLimit;
}
