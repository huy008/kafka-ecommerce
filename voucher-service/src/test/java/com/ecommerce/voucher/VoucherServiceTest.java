package com.ecommerce.voucher;

import com.ecommerce.voucher.dto.ValidateVoucherRequest;
import com.ecommerce.voucher.dto.ValidateVoucherResponse;
import com.ecommerce.voucher.entity.VoucherEntity;
import com.ecommerce.voucher.entity.VoucherUsageEntity;
import com.ecommerce.voucher.repository.VoucherRepository;
import com.ecommerce.voucher.repository.VoucherUsageRepository;
import com.ecommerce.voucher.service.VoucherService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class VoucherServiceTest {

    @Mock
    private VoucherRepository voucherRepository;

    @Mock
    private VoucherUsageRepository voucherUsageRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;
    private VoucherService voucherService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        voucherService = new VoucherService(voucherRepository, voucherUsageRepository, kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("Validates percentage voucher calculation correctly")
    void testValidatePercentageVoucher() {
        VoucherEntity voucher = new VoucherEntity("SALE10", "10% off", "PERCENTAGE", 10.0, 100_000.0, 200_000.0, 100);
        when(voucherRepository.findByCode("SALE10")).thenReturn(Optional.of(voucher));

        ValidateVoucherRequest req = new ValidateVoucherRequest("SALE10", 500_000.0, "CUST-001");
        ValidateVoucherResponse res = voucherService.validateVoucher(req);

        assertTrue(res.isValid());
        assertEquals(50_000.0, res.getDiscountAmount());
        assertEquals(450_000.0, res.getFinalAmount());
    }

    @Test
    @DisplayName("Saga Compensation: Releases voucher usage quota when order fails")
    void testSagaCompensationReleaseVoucher() {
        VoucherEntity voucher = new VoucherEntity("SALE10", "10% off", "PERCENTAGE", 10.0, 100_000.0, 200_000.0, 100);
        voucher.setUsedCount(1);

        VoucherUsageEntity usage = new VoucherUsageEntity("SALE10", "ORD-123", "CUST-001", 500_000.0, 50_000.0, 450_000.0, "APPLIED");

        when(voucherUsageRepository.findByOrderNumberAndStatus("ORD-123", "APPLIED")).thenReturn(Optional.of(usage));
        when(voucherRepository.findByCode("SALE10")).thenReturn(Optional.of(voucher));

        voucherService.releaseVoucherCompensation("ORD-123", "Payment Failed");

        assertEquals(0, voucher.getUsedCount());
        assertEquals("RELEASED", usage.getStatus());
        verify(voucherUsageRepository, times(1)).save(usage);
        verify(voucherRepository, times(1)).save(voucher);
    }
}
