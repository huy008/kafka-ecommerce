package com.ecommerce.voucher.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ValidateVoucherRequest {
    private String voucherCode;
    private Double orderAmount;
    private String customerId;
}
