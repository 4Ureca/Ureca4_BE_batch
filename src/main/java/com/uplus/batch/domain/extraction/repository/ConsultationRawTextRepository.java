package com.uplus.batch.domain.extraction.repository;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.uplus.batch.domain.extraction.entity.ConsultationRawText;

public interface ConsultationRawTextRepository extends JpaRepository<ConsultationRawText, Long> {
    Optional<ConsultationRawText> findByConsultId(Long consultId);
}