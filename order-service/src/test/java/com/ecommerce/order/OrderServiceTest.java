package com.ecommerce.order;

import com.ecommerce.order.dto.CreateOrderRequest;
import com.ecommerce.order.entity.OrderEntity;
import com.ecommerce.order.entity.OutboxEventEntity;
import com.ecommerce.order.repository.OrderRepository;
import com.ecommerce.order.repository.OutboxEventRepository;
import com.ecommerce.order.service.OrderService;
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
    @DisplayName("Transactional Outbox: Ghi Order và OutboxEvent cùng 1 Transaction")
    void testCreateOrderTransactionalOutbox() {
        CreateOrderRequest request = new CreateOrderRequest("USER_01", "MacBook Pro", 1, 45000000.0, 45000000.0, "CREDIT_CARD", "HN");
        when(orderRepository.save(any(OrderEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderEntity created = orderService.createOrder(request);

        assertNotNull(created);
        assertEquals("PENDING", created.getStatus());

        ArgumentCaptor<OutboxEventEntity> captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(outboxEventRepository, times(1)).save(captor.capture());

        OutboxEventEntity outbox = captor.getValue();
        assertEquals("ORDER", outbox.getAggregateType());
        assertEquals("PENDING", outbox.getStatus());
        assertTrue(outbox.getPayload().contains("MacBook Pro"));
    }
}

