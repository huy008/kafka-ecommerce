package com.ecommerce.voucher.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ValidateVoucherResponse {
    private boolean valid;
    private String voucherCode;
    private Double originalAmount;
    private Double discountAmount;
    private Double finalAmount;
    private String message;

    public static ValidateVoucherResponse invalid(String voucherCode, Double originalAmount, String message) {
        return new ValidateVoucherResponse(false, voucherCode, originalAmount, 0.0, originalAmount, message);
    }

    public static ValidateVoucherResponse valid(String voucherCode, Double originalAmount, Double discountAmount, Double finalAmount, String message) {
        return new ValidateVoucherResponse(true, voucherCode, originalAmount, discountAmount, finalAmount, message);
    }
}
