package com.example.ecommerce.repository;

import com.example.ecommerce.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {
    List<OutboxEventEntity> findTop20ByStatusOrderByCreatedAtAsc(String status);
    List<OutboxEventEntity> findTop50ByOrderByCreatedAtDesc();
    List<OutboxEventEntity> findByStatus(String status);
    long countByStatus(String status);
}
