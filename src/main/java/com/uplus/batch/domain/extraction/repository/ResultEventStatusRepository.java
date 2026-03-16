package com.uplus.batch.domain.extraction.repository;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.uplus.batch.domain.extraction.entity.EventStatus;
import com.uplus.batch.domain.extraction.entity.ResultEventStatus;

public interface ResultEventStatusRepository extends JpaRepository<ResultEventStatus, Long> {
    List<ResultEventStatus> findTop50ByStatusOrderByCreatedAtAsc(EventStatus status);
    List<ResultEventStatus> findByStatusAndRetryCountLessThan(EventStatus status, int retryLimit);
    @Query("""
            SELECT r FROM ResultEventStatus r 
            JOIN ExcellentEventStatus e ON r.consultId = e.consultId 
            WHERE r.status = 'REQUESTED' AND e.status = 'REQUESTED' 
            ORDER BY r.createdAt ASC
        """)
        List<ResultEventStatus> findReadyToProcessPairs(Pageable pageable);
    
    @Modifying
    @Query("UPDATE ResultEventStatus r SET r.status = 'READY' " +
           "WHERE r.status = 'PROCESSING' AND r.updatedAt < :threshold")
    int cleanupStaleProcessingTasks(@Param("threshold") LocalDateTime threshold);
}