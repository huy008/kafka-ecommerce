package com.example.ecommerce.repository;

import com.example.ecommerce.entity.DeadLetterEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEventEntity, Long> {
    List<DeadLetterEventEntity> findTop50ByOrderByReceivedAtDesc();
    List<DeadLetterEventEntity> findByStatus(String status);
    long countByStatus(String status);
}

