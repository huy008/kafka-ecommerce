package com.example.ecommerce.service;

import com.example.ecommerce.dto.CreateOrderRequest;
import com.example.ecommerce.entity.OrderEntity;
import com.example.ecommerce.entity.OutboxEventEntity;
import com.example.ecommerce.repository.OrderRepository;
import com.example.ecommerce.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private ObjectMapper objectMapper;
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        orderService = new OrderService(orderRepository, outboxEventRepository, objectMapper);
    }

    @Test
    @DisplayName("Transactional Outbox: Tạo Order phải đồng thời ghi nhận Outbox Event ở trạng thái PENDING")
    void testCreateOrderWithTransactionalOutbox() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "USER_TEST_01",
                "MacBook Pro M3 Max",
                1,
                65000000.0,
                65000000.0,
                "CREDIT_CARD",
                "Hà Nội"
        );

        when(orderRepository.save(any(OrderEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // When
        OrderEntity result = orderService.createOrder(request);

        // Then
        assertNotNull(result);
        assertNotNull(result.getOrderNumber());
        assertEquals("USER_TEST_01", result.getCustomerId());
        assertEquals("PENDING", result.getStatus());
        assertEquals(65000000.0, result.getAmount());

        // Kiểm tra xem Outbox Event có được lưu không
        ArgumentCaptor<OutboxEventEntity> outboxCaptor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(outboxEventRepository, times(1)).save(outboxCaptor.capture());

        OutboxEventEntity capturedOutbox = outboxCaptor.getValue();
        assertNotNull(capturedOutbox.getEventId());
        assertEquals("ORDER", capturedOutbox.getAggregateType());
        assertEquals(result.getOrderNumber(), capturedOutbox.getAggregateId());
        assertEquals("ORDER_CREATED", capturedOutbox.getEventType());
        assertEquals("PENDING", capturedOutbox.getStatus());
        assertNotNull(capturedOutbox.getPayload());
        assertTrue(capturedOutbox.getPayload().contains("MacBook Pro M3 Max"));
    }
}

