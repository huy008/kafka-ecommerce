package com.ecommerce.inventory.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name = "inventory_stock")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryStockEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String productName;

    private Integer totalStock;
    @Builder.Default
    private Integer reservedStock = 0;
    private Integer availableStock;

    public InventoryStockEntity(String productName, Integer totalStock) {
        this.productName = productName;
        this.totalStock = totalStock;
        this.reservedStock = 0;
        this.availableStock = totalStock;
    }

    public boolean reserve(int quantity) {
        if (availableStock != null && availableStock >= quantity) {
            availableStock -= quantity;
            reservedStock += quantity;
            return true;
        }
        return false;
    }

    public void release(int quantity) {
        if (reservedStock != null && reservedStock >= quantity) {
            reservedStock -= quantity;
            availableStock += quantity;
        }
    }
}
