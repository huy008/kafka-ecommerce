package com.ecommerce.payment;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.InventoryReservedEvent;
import com.ecommerce.payment.entity.PaymentEntity;
import com.ecommerce.payment.repository.PaymentRepository;
import com.ecommerce.payment.repository.ProcessedEventRepository;
import com.ecommerce.payment.service.PaymentService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;
    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        paymentService = new PaymentService(paymentRepository, processedEventRepository, kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("Saga Choreography: Thanh toán thành công phát sinh PaymentSuccessEvent")
    void testProcessPaymentSuccess() {
        InventoryReservedEvent event = new InventoryReservedEvent("EVT-RES-1", "ORD-1", "USER_VIP", "MacBook Pro", 1, 45000000.0, "CREDIT_CARD");
        when(processedEventRepository.existsById("EVT-RES-1")).thenReturn(false);

        paymentService.processPayment(event);

        ArgumentCaptor<PaymentEntity> captor = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(paymentRepository, times(1)).save(captor.capture());

        PaymentEntity saved = captor.getValue();
        assertEquals("SUCCESS", saved.getStatus());
        assertEquals("ORD-1", saved.getOrderNumber());

        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicConstants.TOPIC_PAYMENT_SUCCESS), eq("USER_VIP"), any());
    }

    @Test
    @DisplayName("Saga Compensation: Khi thanh toán thất bại, kích hoạt InventoryReleaseCommand để xả kho")
    void testProcessPaymentFailureTriggersCompensation() {
        // User FAIL_PAYMENT_USER sẽ kích hoạt lỗi thẻ
        InventoryReservedEvent event = new InventoryReservedEvent("EVT-RES-2", "ORD-2", "FAIL_PAYMENT_USER", "iPhone 16", 1, 34000000.0, "CREDIT_CARD");
        when(processedEventRepository.existsById("EVT-RES-2")).thenReturn(false);

        paymentService.processPayment(event);

        ArgumentCaptor<PaymentEntity> captor = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(paymentRepository, times(1)).save(captor.capture());

        PaymentEntity saved = captor.getValue();
        assertEquals("FAILED", saved.getStatus());

        // Bắn PaymentFailedEvent
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicConstants.TOPIC_PAYMENT_FAILED), eq("FAIL_PAYMENT_USER"), any());

        // KÍCH HOẠT LỆNH BÙ TRỪ XẢ KHO SANG TOPIC inventory-release-events!
        verify(kafkaTemplate, times(1)).send(eq(KafkaTopicConstants.TOPIC_INVENTORY_RELEASE), eq("FAIL_PAYMENT_USER"), any());
    }
}

