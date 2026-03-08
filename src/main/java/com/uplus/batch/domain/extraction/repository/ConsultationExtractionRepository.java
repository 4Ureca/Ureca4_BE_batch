package com.uplus.batch.domain.extraction.repository;

import com.uplus.batch.domain.extraction.entity.ConsultationExtraction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsultationExtractionRepository extends JpaRepository<ConsultationExtraction, Long> {
}