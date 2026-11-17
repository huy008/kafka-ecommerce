package com.ecommerce.voucher.repository;

import com.ecommerce.voucher.entity.VoucherUsageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface VoucherUsageRepository extends JpaRepository<VoucherUsageEntity, Long> {
    Optional<VoucherUsageEntity> findByOrderNumberAndStatus(String orderNumber, String status);
    List<VoucherUsageEntity> findByOrderNumber(String orderNumber);
    List<VoucherUsageEntity> findTop50ByOrderByCreatedAtDesc();
    long countByStatus(String status);
}

