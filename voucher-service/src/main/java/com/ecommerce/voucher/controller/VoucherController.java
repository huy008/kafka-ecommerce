package com.ecommerce.voucher.controller;

import com.ecommerce.voucher.dto.*;
import com.ecommerce.voucher.entity.VoucherEntity;
import com.ecommerce.voucher.entity.VoucherUsageEntity;
import com.ecommerce.voucher.service.VoucherService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/vouchers")
@RequiredArgsConstructor
@Tag(name = "Voucher Management API", description = "Voucher promotions, quotas, and Saga compensation")
public class VoucherController {

    private final VoucherService voucherService;

    @GetMapping
    @Operation(summary = "Get list of all vouchers")
    public ResponseEntity<List<VoucherEntity>> getVouchers() {
        return ResponseEntity.ok(voucherService.getAllVouchers());
    }

    @GetMapping("/usages")
    @Operation(summary = "Get voucher usage history")
    public ResponseEntity<List<VoucherUsageEntity>> getUsages() {
        return ResponseEntity.ok(voucherService.getRecentUsages());
    }

    @PostMapping
    @Operation(summary = "Create a new voucher")
    public ResponseEntity<VoucherEntity> createVoucher(@RequestBody CreateVoucherRequest request) {
        return ResponseEntity.ok(voucherService.createVoucher(request));
    }

    @PostMapping("/validate")
    @Operation(summary = "Validate voucher code and calculate discount amount")
    public ResponseEntity<ValidateVoucherResponse> validateVoucher(@RequestBody ValidateVoucherRequest request) {
        return ResponseEntity.ok(voucherService.validateVoucher(request));
    }

    @PostMapping("/apply")
    @Operation(summary = "Apply voucher code to order")
    public ResponseEntity<ValidateVoucherResponse> applyVoucher(@RequestBody ApplyVoucherRequest request) {
        return ResponseEntity.ok(voucherService.applyVoucher(request));
    }

    @PostMapping("/release")
    @Operation(summary = "Compensate/Release voucher (Saga Compensation)")
    public ResponseEntity<Map<String, String>> releaseVoucher(@RequestParam String orderNumber,
                                                             @RequestParam(required = false, defaultValue = "Manual cancel") String reason) {
        voucherService.releaseVoucherCompensation(orderNumber, reason);
        return ResponseEntity.ok(Map.of("message", "Voucher release command executed for order: " + orderNumber));
    }

    @GetMapping("/metrics")
    @Operation(summary = "Get voucher statistics and usage counts")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        return ResponseEntity.ok(voucherService.getMetrics());
    }
}
