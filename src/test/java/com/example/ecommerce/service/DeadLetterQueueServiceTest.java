package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.DeadLetterEventEntity;
import com.example.ecommerce.repository.DeadLetterEventRepository;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeadLetterQueueServiceTest {

    @Mock
    private DeadLetterEventRepository deadLetterEventRepository;

    @Mock
    private OrderService orderService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;
    private DeadLetterQueueService dlqService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        dlqService = new DeadLetterQueueService(deadLetterEventRepository, orderService, kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("DLQ Replay: Khắc phục bản tin hỏng, cấp eventId mới và phát lại vào topic order-events")
    void testReplayDeadLetterSuccess() throws Exception {
        // Given
        Long dltId = 1L;
        OrderEventDTO originalDto = new OrderEventDTO(
                "EVT-OLD-FAIL",
                "ORD-FAIL-01",
                "USER_DLT",
                "FAIL_INVENTORY_IPHONE_16",
                30000000.0
        );
        String payload = objectMapper.writeValueAsString(originalDto);

        DeadLetterEventEntity deadLetter = new DeadLetterEventEntity(
                "EVT-OLD-FAIL",
                "ORD-FAIL-01",
                KafkaTopicAndStreamsConfig.TOPIC_ORDERS_DLT,
                0,
                100L,
                payload,
                "Kho báo hết hàng"
        );

        when(deadLetterEventRepository.findById(dltId)).thenReturn(Optional.of(deadLetter));

        // When
        boolean result = dlqService.replayDeadLetter(dltId);

        // Then
        assertTrue(result);
        assertEquals("REPLAYED", deadLetter.getStatus());
        assertNotNull(deadLetter.getReplayedAt());
        verify(deadLetterEventRepository, times(1)).save(deadLetter);

        // Đơn hàng được đổi về PENDING
        verify(orderService, times(1)).updateOrderStatus(eq("ORD-FAIL-01"), eq("PENDING"), any());

        // Gửi lại lên Kafka
        verify(kafkaTemplate, times(1)).send(
                eq(KafkaTopicAndStreamsConfig.TOPIC_ORDERS),
                eq("USER_DLT"),
                contains("RESTOCKED_IPHONE_16")
        );
    }
}

