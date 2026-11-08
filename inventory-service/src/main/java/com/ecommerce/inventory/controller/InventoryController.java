package com.ecommerce.inventory.controller;

import com.ecommerce.inventory.entity.InventoryStockEntity;
import com.ecommerce.inventory.repository.InventoryStockRepository;
import com.ecommerce.inventory.repository.ProcessedEventRepository;
import com.ecommerce.inventory.service.InventoryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/inventory")
@Tag(name = "Inventory Management API", description = "Quản lý tồn kho và giao dịch bù trừ Saga")
public class InventoryController {

    private final InventoryService inventoryService;
    private final InventoryStockRepository stockRepository;
    private final ProcessedEventRepository processedEventRepository;

    public InventoryController(InventoryService inventoryService,
                               InventoryStockRepository stockRepository,
                               ProcessedEventRepository processedEventRepository) {
        this.inventoryService = inventoryService;
        this.stockRepository = stockRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @GetMapping("/stocks")
    @Operation(summary = "Xem danh sách tồn kho thời gian thực")
    public ResponseEntity<List<InventoryStockEntity>> getStocks() {
        return ResponseEntity.ok(inventoryService.getAllStocks());
    }

    @PostMapping("/restock")
    @Operation(summary = "Nhập thêm hàng vào kho")
    public ResponseEntity<Map<String, Object>> restock(@RequestParam String productName, @RequestParam int quantity) {
        InventoryStockEntity stock = stockRepository.findByProductName(productName)
                .orElse(new InventoryStockEntity(productName, 0));
        stock.setTotalStock(stock.getTotalStock() + quantity);
        stock.setAvailableStock(stock.getAvailableStock() + quantity);
        stockRepository.save(stock);

        Map<String, Object> res = new HashMap<>();
        res.put("status", "SUCCESS");
        res.put("productName", productName);
        res.put("newAvailableStock", stock.getAvailableStock());
        return ResponseEntity.ok(res);
    }

    @GetMapping("/metrics")
    @Operation(summary = "Thống kê Inventory Service")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalProducts", stockRepository.count());
        metrics.put("idempotentEventsProcessed", processedEventRepository.count());
        return ResponseEntity.ok(metrics);
    }
}

