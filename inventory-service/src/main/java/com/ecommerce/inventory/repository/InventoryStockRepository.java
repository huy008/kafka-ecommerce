package com.ecommerce.inventory.repository;

import com.ecommerce.inventory.entity.InventoryStockEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InventoryStockRepository extends JpaRepository<InventoryStockEntity, Long> {
    Optional<InventoryStockEntity> findByProductName(String productName);
}

