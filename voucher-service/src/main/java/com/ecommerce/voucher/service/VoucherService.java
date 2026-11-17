package com.ecommerce.voucher.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.voucher.dto.*;
import com.ecommerce.voucher.entity.VoucherEntity;
import com.ecommerce.voucher.entity.VoucherUsageEntity;
import com.ecommerce.voucher.repository.VoucherRepository;
import com.ecommerce.voucher.repository.VoucherUsageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class VoucherService {

    private final VoucherRepository voucherRepository;
    private final VoucherUsageRepository voucherUsageRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @PostConstruct
    public void initDefaultVouchers() {
        if (voucherRepository.count() == 0) {
            log.info("🎟️ [VoucherService] Initializing default seed vouchers...");
            voucherRepository.save(new VoucherEntity(
                    "SALE10", "10% off up to 100,000 VND for orders from 200,000 VND",
                    "PERCENTAGE", 10.0, 100_000.0, 200_000.0, 100
            ));
            voucherRepository.save(new VoucherEntity(
                    "GIAM50K", "Flat 50,000 VND discount for orders from 300,000 VND",
                    "FIXED_AMOUNT", 50_000.0, 50_000.0, 300_000.0, 50
            ));
            voucherRepository.save(new VoucherEntity(
                    "FREESHIP", "Free shipping support of 30,000 VND for orders from 150,000 VND",
                    "FIXED_AMOUNT", 30_000.0, 30_000.0, 150_000.0, 200
            ));
            voucherRepository.save(new VoucherEntity(
                    "VIP2026", "VIP Exclusive: 20% off up to 500,000 VND for orders from 500,000 VND",
                    "PERCENTAGE", 20.0, 500_000.0, 500_000.0, 20
            ));
            log.info("✅ [VoucherService] Initialized 4 default vouchers successfully!");
        }
    }

    public List<VoucherEntity> getAllVouchers() {
        return voucherRepository.findAll();
    }

    public List<VoucherUsageEntity> getRecentUsages() {
        return voucherUsageRepository.findTop50ByOrderByCreatedAtDesc();
    }

    @Transactional
    public VoucherEntity createVoucher(CreateVoucherRequest req) {
        if (voucherRepository.existsByCode(req.getCode().toUpperCase())) {
            throw new IllegalArgumentException("Voucher code '" + req.getCode() + "' already exists!");
        }
        VoucherEntity entity = new VoucherEntity(
                req.getCode().toUpperCase(),
                req.getDescription(),
                req.getDiscountType() != null ? req.getDiscountType().toUpperCase() : "FIXED_AMOUNT",
                req.getDiscountValue(),
                req.getMaxDiscountAmount(),
                req.getMinOrderAmount(),
                req.getUsageLimit()
        );
        return voucherRepository.save(entity);
    }

    public ValidateVoucherResponse validateVoucher(ValidateVoucherRequest req) {
        if (req.getVoucherCode() == null || req.getVoucherCode().trim().isEmpty()) {
            return ValidateVoucherResponse.invalid("", req.getOrderAmount(), "Please enter a voucher code!");
        }
        String code = req.getVoucherCode().trim().toUpperCase();
        double orderAmount = req.getOrderAmount() != null ? req.getOrderAmount() : 0.0;

        Optional<VoucherEntity> opt = voucherRepository.findByCode(code);
        if (!opt.isPresent()) {
            return ValidateVoucherResponse.invalid(code, orderAmount, "Voucher code does not exist!");
        }

        VoucherEntity voucher = opt.get();
        if (!"ACTIVE".equalsIgnoreCase(voucher.getStatus())) {
            return ValidateVoucherResponse.invalid(code, orderAmount, "Voucher is not active or has exhausted its quota!");
        }
        if (voucher.getMinOrderAmount() != null && orderAmount < voucher.getMinOrderAmount()) {
            return ValidateVoucherResponse.invalid(code, orderAmount,
                    String.format("Minimum order amount must be at least %,.0f VND to apply this voucher!", voucher.getMinOrderAmount()));
        }
        if (voucher.getUsageLimit() != null && voucher.getUsedCount() >= voucher.getUsageLimit()) {
            return ValidateVoucherResponse.invalid(code, orderAmount, "Voucher has reached its usage limit!");
        }

        double discount = voucher.calculateDiscount(orderAmount);
        double finalAmount = Math.max(0.0, orderAmount - discount);

        return ValidateVoucherResponse.valid(code, orderAmount, discount, finalAmount,
                String.format("Voucher applied successfully! Discount: %,.0f VND", discount));
    }

    @Transactional
    public ValidateVoucherResponse applyVoucher(ApplyVoucherRequest req) {
        String code = req.getVoucherCode().trim().toUpperCase();
        String orderNumber = req.getOrderNumber();
        double orderAmount = req.getOrderAmount() != null ? req.getOrderAmount() : 0.0;

        // Idempotency: If this order has already applied this voucher and status is APPLIED
        Optional<VoucherUsageEntity> existingUsage = voucherUsageRepository.findByOrderNumberAndStatus(orderNumber, "APPLIED");
        if (existingUsage.isPresent()) {
            VoucherUsageEntity usage = existingUsage.get();
            log.info("🛡️ [Voucher Idempotent] Order {} has already applied voucher {}. Skipping duplicate!", orderNumber, code);
            return ValidateVoucherResponse.valid(code, orderAmount, usage.getDiscountAmount(), usage.getFinalAmount(), "Already applied previously");
        }

        VoucherEntity voucher = voucherRepository.findByCode(code)
                .orElseThrow(() -> new IllegalArgumentException("Voucher not found: " + code));

        if (!voucher.canUse(orderAmount)) {
            throw new IllegalStateException("Voucher is invalid or does not meet usage criteria!");
        }

        double discount = voucher.calculateDiscount(orderAmount);
        double finalAmount = Math.max(0.0, orderAmount - discount);

        boolean incremented = voucher.incrementUsage();
        if (!incremented) {
            throw new IllegalStateException("Voucher exhausted right before placement!");
        }
        voucherRepository.save(voucher);

        VoucherUsageEntity usage = new VoucherUsageEntity(
                code, orderNumber, req.getCustomerId(), orderAmount, discount, finalAmount, "APPLIED"
        );
        voucherUsageRepository.save(usage);

        log.info("🎟️ [VoucherService] Applied voucher {} to order {}. Discount: {} VND -> Remaining: {} VND",
                code, orderNumber, discount, finalAmount);

        // Publish event to Kafka
        try {
            Map<String, Object> eventPayload = new HashMap<>();
            eventPayload.put("eventId", "EVT-VOUCHER-" + UUID.randomUUID().toString().substring(0, 8));
            eventPayload.put("orderNumber", orderNumber);
            eventPayload.put("voucherCode", code);
            eventPayload.put("discountAmount", discount);
            eventPayload.put("finalAmount", finalAmount);
            kafkaTemplate.send(KafkaTopicConstants.TOPIC_VOUCHER_APPLIED, req.getCustomerId(), objectMapper.writeValueAsString(eventPayload));
        } catch (Exception e) {
            log.warn("Error sending Kafka event TOPIC_VOUCHER_APPLIED: {}", e.getMessage());
        }

        return ValidateVoucherResponse.valid(code, orderAmount, discount, finalAmount, "Voucher applied successfully!");
    }

    /**
     * SAGA COMPENSATING TRANSACTIONS:
     * When an order is cancelled or payment fails -> Release voucher usage count back.
     */
    @Transactional
    public void releaseVoucherCompensation(String orderNumber, String reason) {
        Optional<VoucherUsageEntity> optUsage = voucherUsageRepository.findByOrderNumberAndStatus(orderNumber, "APPLIED");
        if (!optUsage.isPresent()) {
            return; // Order did not use a voucher or was already released
        }

        VoucherUsageEntity usage = optUsage.get();
        usage.setStatus("RELEASED");
        voucherUsageRepository.save(usage);

        voucherRepository.findByCode(usage.getVoucherCode()).ifPresent(voucher -> {
            voucher.decrementUsage();
            voucherRepository.save(voucher);
            log.warn("⚡ [Saga Compensation - Voucher] Released 1 usage quota back for voucher {} (Order: {}, Reason: {})",
                    usage.getVoucherCode(), orderNumber, reason);
        });

        // Publish compensation event to Kafka
        try {
            Map<String, Object> eventPayload = new HashMap<>();
            eventPayload.put("eventId", "EVT-VOUCHER-REL-" + UUID.randomUUID().toString().substring(0, 8));
            eventPayload.put("orderNumber", orderNumber);
            eventPayload.put("voucherCode", usage.getVoucherCode());
            eventPayload.put("reason", reason);
            kafkaTemplate.send(KafkaTopicConstants.TOPIC_VOUCHER_RELEASE, usage.getCustomerId(), objectMapper.writeValueAsString(eventPayload));
        } catch (Exception e) {
            log.warn("Error sending Kafka event TOPIC_VOUCHER_RELEASE: {}", e.getMessage());
        }
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalVouchers", voucherRepository.count());
        metrics.put("activeVouchers", voucherRepository.countByStatus("ACTIVE"));
        metrics.put("exhaustedVouchers", voucherRepository.countByStatus("EXHAUSTED"));
        metrics.put("totalUsages", voucherUsageRepository.count());
        metrics.put("appliedUsages", voucherUsageRepository.countByStatus("APPLIED"));
        metrics.put("releasedUsages", voucherUsageRepository.countByStatus("RELEASED"));
        return metrics;
    }
}
