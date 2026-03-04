package com.uplus.batch.domain.extraction.repository;

import com.uplus.batch.domain.extraction.entity.ResultEventStatus;
import com.uplus.batch.domain.extraction.entity.EventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface EventStatusRepository extends JpaRepository<ResultEventStatus, Long> {
    // 상태가 REQUESTED인 것들 중 오래된 순으로 50개만 가져오기
    List<ResultEventStatus> findTop50ByStatusOrderByCreatedAtAsc(EventStatus status);
}