package com.uplus.batch.domain.extraction.repository;

import com.uplus.batch.domain.extraction.entity.ConsultationExtraction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsultationExtractionRepository extends JpaRepository<ConsultationExtraction, Long> {
    // consult_id가 PK이므로 기본 제공되는 save, findById로 충분합니다.
}