package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.OrderEntity;
import com.example.ecommerce.entity.ProcessedEventEntity;
import com.example.ecommerce.repository.OrderRepository;
import com.example.ecommerce.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentAndInventoryConsumerTest {

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;
    private PaymentAndInventoryConsumer consumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        consumer = new PaymentAndInventoryConsumer(processedEventRepository, orderRepository, kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("Idempotent Consumer: Xử lý thành công đơn hàng lần đầu và lưu vào processed_events")
    void testProcessOrderEventSuccess() throws Exception {
        // Given
        String eventId = "EVT-TEST-123";
        String orderNumber = "ORD-TEST-123";
        OrderEventDTO dto = new OrderEventDTO(eventId, orderNumber, "USER_01", "Bàn phím cơ", 2500000.0);
        String jsonPayload = objectMapper.writeValueAsString(dto);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                KafkaTopicAndStreamsConfig.TOPIC_ORDERS, 0, 10L, "USER_01", jsonPayload
        );

        when(processedEventRepository.existsById(eventId)).thenReturn(false);

        OrderEntity mockOrder = new OrderEntity(orderNumber, "USER_01", "Bàn phím cơ", 2500000.0, "PENDING");
        when(orderRepository.findByOrderNumber(orderNumber)).thenReturn(Optional.of(mockOrder));

        // When
        consumer.processOrderEvent(record);

        // Then: Đã lưu vào processed_events để đánh dấu Idempotent
        verify(processedEventRepository, times(1)).save(any(ProcessedEventEntity.class));

        // Trạng thái đơn đổi sang CONFIRMED
        assertEquals("CONFIRMED", mockOrder.getStatus());
        verify(orderRepository, times(1)).save(mockOrder);

        // Bắn sự kiện thanh toán thành công lên Kafka
        verify(kafkaTemplate, times(1)).send(
                eq(KafkaTopicAndStreamsConfig.TOPIC_PAYMENT_CONFIRMED),
                eq("USER_01"),
                any(String.class)
        );
    }

    @Test
    @DisplayName("Idempotent Guard: Nếu eventId đã tồn tại, lập tức bỏ qua không trừ tiền lần hai")
    void testProcessOrderEventDuplicateSkipped() throws Exception {
        // Given
        String duplicateEventId = "EVT-DUPLICATE-999";
        OrderEventDTO dto = new OrderEventDTO(duplicateEventId, "ORD-999", "USER_02", "iPhone 16", 32000000.0);
        String jsonPayload = objectMapper.writeValueAsString(dto);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                KafkaTopicAndStreamsConfig.TOPIC_ORDERS, 0, 15L, "USER_02", jsonPayload
        );

        // Giả lập eventId đã tồn tại trong database
        when(processedEventRepository.existsById(duplicateEventId)).thenReturn(true);

        // When
        consumer.processOrderEvent(record);

        // Then: Không được lưu lại vào processed_events, không đổi trạng thái đơn, không gửi Kafka
        verify(processedEventRepository, never()).save(any(ProcessedEventEntity.class));
        verify(orderRepository, never()).save(any(OrderEntity.class));
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    @DisplayName("DLQ Trigger: Sản phẩm FAIL_INVENTORY phải ném lỗi để kích hoạt DefaultErrorHandler")
    void testProcessOrderEventThrowsForDlq() throws Exception {
        // Given
        String eventId = "EVT-POISON-001";
        OrderEventDTO dto = new OrderEventDTO(eventId, "ORD-POISON", "USER_03", "FAIL_INVENTORY_LIMITED_ITEM", 50000000.0);
        String jsonPayload = objectMapper.writeValueAsString(dto);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                KafkaTopicAndStreamsConfig.TOPIC_ORDERS, 0, 20L, "USER_03", jsonPayload
        );

        when(processedEventRepository.existsById(eventId)).thenReturn(false);

        // When & Then
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> consumer.processOrderEvent(record));
        assertTrue(thrown.getMessage().contains("Kho báo hết hàng"));
    }
}

