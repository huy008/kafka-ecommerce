package com.ecommerce.voucher.repository;

import com.ecommerce.voucher.entity.VoucherEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface VoucherRepository extends JpaRepository<VoucherEntity, Long> {
    Optional<VoucherEntity> findByCode(String code);
    boolean existsByCode(String code);
    long countByStatus(String status);
}

