package com.uplus.batch.domain.extraction.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.uplus.batch.domain.extraction.entity.ConsultationRawText;

public interface ConsultationRawTextRepository extends JpaRepository<ConsultationRawText, Long> {
    Optional<ConsultationRawText> findByConsultId(Long consultId);

    /** 번들 처리용 — consultId 목록으로 원문 일괄 조회 */
    List<ConsultationRawText> findAllByConsultIdIn(List<Long> consultIds);
}