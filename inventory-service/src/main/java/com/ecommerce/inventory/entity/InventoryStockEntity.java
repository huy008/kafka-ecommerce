package com.ecommerce.inventory.entity;

import javax.persistence.*;

@Entity
@Table(name = "inventory_stock")
public class InventoryStockEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String productName;

    private Integer totalStock;
    private Integer reservedStock;
    private Integer availableStock;

    public InventoryStockEntity() {}

    public InventoryStockEntity(String productName, Integer totalStock) {
        this.productName = productName;
        this.totalStock = totalStock;
        this.reservedStock = 0;
        this.availableStock = totalStock;
    }

    public Long getId() { return id; }

    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }

    public Integer getTotalStock() { return totalStock; }
    public void setTotalStock(Integer totalStock) { this.totalStock = totalStock; }

    public Integer getReservedStock() { return reservedStock; }
    public void setReservedStock(Integer reservedStock) { this.reservedStock = reservedStock; }

    public Integer getAvailableStock() { return availableStock; }
    public void setAvailableStock(Integer availableStock) { this.availableStock = availableStock; }

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

