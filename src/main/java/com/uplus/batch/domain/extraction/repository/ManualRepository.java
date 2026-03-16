package com.uplus.batch.domain.extraction.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.uplus.batch.domain.extraction.entity.Manual;

public interface ManualRepository extends JpaRepository<Manual, Integer> {
    // 이제 categoryCode가 String이므로 쿼리가 훨씬 단순해짐
    Optional<Manual> findByCategoryCodeAndIsActiveTrue(String categoryCode);
}