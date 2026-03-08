package com.uplus.batch.domain.extraction.repository;

import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.entity.EventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface EventStatusRepository extends JpaRepository<ResultEventStatus, Long> {
    List<ResultEventStatus> findTop50ByStatusOrderByCreatedAtAsc(EventStatus status);
    List<ResultEventStatus> findByStatusAndRetryCountLessThan(EventStatus status, int retryLimit);
}