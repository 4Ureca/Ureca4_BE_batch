package com.uplus.batch.domain.summary.service;

import com.uplus.batch.domain.summary.entity.SummaryEventStatus;
import com.uplus.batch.domain.summary.repository.SummaryEventStatusRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

/**
 * 상담 원문/결과서가 저장된 consultation_results를 기반으로
 * result_event_status와 summary_event_status에 'requested' 상태를 발행한다.
 *
 * <p>AI 요약 처리 및 ES 등록은 별도 배치 스케줄러가 'requested' 상태를 감지하여 수행한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConsultationSummaryGenerator {

    private final SummaryEventStatusRepository summaryEventStatusRepo;
    private final JdbcTemplate jdbcTemplate;

    /**
     * startId ~ endId 범위의 consultation_results를 대상으로
     * result_event_status(REQUESTED)와 summary_event_status(requested)를 삽입한다.
     *
     * @return 처리된 건수
     */
    @Transactional
    public int publishEvents(Long startId, Long endId) {
        List<Object[]> rawRows = jdbcTemplate.query(
                "SELECT consult_id, category_code FROM consultation_results WHERE consult_id BETWEEN ? AND ?",
                (rs, rowNum) -> new Object[]{rs.getLong("consult_id"), rs.getString("category_code")},
                startId, endId
        );

        if (rawRows.isEmpty()) {
            log.info("[SummaryGenerator] 처리할 대상 없음 (range: {} ~ {})", startId, endId);
            return 0;
        }

        List<Long> consultIds = new ArrayList<>(rawRows.size());
        List<Object[]> resultEventArgs = new ArrayList<>(rawRows.size());
        List<Object[]> excellentEventArgs = new ArrayList<>(rawRows.size());
        
        for (Object[] row : rawRows) {
            Long consultId = (Long) row[0];
            String categoryCode = (String) row[1];
            consultIds.add(consultId);
            resultEventArgs.add(new Object[]{consultId, categoryCode});
            excellentEventArgs.add(new Object[]{consultId});
        }

        // 1. result_event_status INSERT (REQUESTED)
        jdbcTemplate.batchUpdate(
                "INSERT INTO result_event_status " +
                "(consult_id, category_code, status, retry_count, created_at, updated_at) " +
                "VALUES (?, ?, 'REQUESTED', 0, NOW(), NOW())",
                resultEventArgs
        );
        
        // 2. excellent_event_status INSERT (REQUESTED)
        jdbcTemplate.batchUpdate(
                "INSERT INTO excellent_event_status " +
                "(consult_id, status, retry_count, created_at, updated_at) " +
                "VALUES (?, 'REQUESTED', 0, NOW(), NOW())",
                excellentEventArgs
        );

        // 3. summary_event_status INSERT (requested)
        List<SummaryEventStatus> summaryEvents = new ArrayList<>(consultIds.size());
        for (Long consultId : consultIds) {
            summaryEvents.add(SummaryEventStatus.builder().consultId(consultId).build());
        }
        summaryEventStatusRepo.saveAll(summaryEvents);

        log.info("[SummaryGenerator] {}건 이벤트 발행 완료 (requested) | range: {} ~ {}",
                rawRows.size(), startId, endId);
        return rawRows.size();
        
        
    }
}
