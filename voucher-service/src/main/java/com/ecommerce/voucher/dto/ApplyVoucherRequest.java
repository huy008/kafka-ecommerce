package com.ecommerce.voucher.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApplyVoucherRequest {
    private String voucherCode;
    private String orderNumber;
    private String customerId;
    private Double orderAmount;
}
