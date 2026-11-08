package com.ecommerce.inventory;

import com.ecommerce.common.event.InventoryReleaseCommand;
import com.ecommerce.common.event.OrderCreatedEvent;
import com.ecommerce.inventory.entity.InventoryStockEntity;
import com.ecommerce.inventory.repository.InventoryStockRepository;
import com.ecommerce.inventory.repository.ProcessedEventRepository;
import com.ecommerce.inventory.service.InventoryService;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private InventoryStockRepository stockRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;
    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        inventoryService = new InventoryService(stockRepository, processedEventRepository, kafkaTemplate, objectMapper);
    }

    @Test
    @DisplayName("Saga Choreography: Giữ kho thành công khi sản phẩm đủ hàng")
    void testReserveStockSuccess() {
        OrderCreatedEvent event = new OrderCreatedEvent("EVT-1", "ORD-1", "CUST-1", "MacBook Pro", 2, 45000000.0, 90000000.0, "CREDIT_CARD", "HN");
        InventoryStockEntity stock = new InventoryStockEntity("MacBook Pro", 10);

        when(processedEventRepository.existsById("EVT-1")).thenReturn(false);
        when(stockRepository.findByProductName("MacBook Pro")).thenReturn(Optional.of(stock));

        inventoryService.reserveStock(event);

        assertEquals(8, stock.getAvailableStock());
        assertEquals(2, stock.getReservedStock());
        verify(stockRepository, times(1)).save(stock);
    }

    @Test
    @DisplayName("Saga Compensation: Xả kho hoàn trả tồn kho khi thanh toán thất bại")
    void testReleaseStockCompensation() {
        InventoryReleaseCommand cmd = new InventoryReleaseCommand("EVT-COMP-1", "ORD-1", "MacBook Pro", 2, "Payment failed");
        InventoryStockEntity stock = new InventoryStockEntity("MacBook Pro", 10);
        stock.setAvailableStock(8);
        stock.setReservedStock(2);

        when(stockRepository.findByProductName("MacBook Pro")).thenReturn(Optional.of(stock));

        inventoryService.releaseStock(cmd);

        assertEquals(10, stock.getAvailableStock());
        assertEquals(0, stock.getReservedStock());
        verify(stockRepository, times(1)).save(stock);
    }
}

