package com.ecommerce.payment.repository;

import com.ecommerce.payment.entity.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PaymentRepository extends JpaRepository<PaymentEntity, Long> {
    List<PaymentEntity> findTop50ByOrderByCreatedAtDesc();
    long countByStatus(String status);
}

