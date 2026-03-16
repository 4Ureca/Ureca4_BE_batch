package com.uplus.batch.domain.extraction.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.uplus.batch.domain.extraction.entity.AnalysisCode;

public interface AnalysisCodeRepository extends JpaRepository<AnalysisCode, Long> {
    List<AnalysisCode> findAllByClassificationIn(List<String> classifications);
}
