package com.uplus.batch.domain.extraction.repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ExcellentEventStatus;

public interface ExcellentEventStatusRepository extends JpaRepository<ExcellentEventStatus, Long> {
    Optional<ExcellentEventStatus> findByConsultId(Long consultId);
    List<ExcellentEventStatus> findByStatusAndRetryCountLessThan(EventStatus status, int retryLimit);
    @Modifying
    @Query("UPDATE ExcellentEventStatus e SET e.status = 'READY' " +
           "WHERE e.status = 'PROCESSING' AND e.updatedAt < :threshold")
    int cleanupStaleProcessingTasks(@Param("threshold") LocalDateTime threshold);
}
