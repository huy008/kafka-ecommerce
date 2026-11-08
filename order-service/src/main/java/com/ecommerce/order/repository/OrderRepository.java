package com.ecommerce.order.repository;

import com.ecommerce.order.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, Long> {
    Optional<OrderEntity> findByOrderNumber(String orderNumber);
    List<OrderEntity> findByCustomerIdOrderByCreatedAtDesc(String customerId);
    List<OrderEntity> findTop50ByOrderByCreatedAtDesc();
    long countByStatus(String status);
}

