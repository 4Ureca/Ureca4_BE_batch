package com.uplus.batch.domain.extraction.controller;

import com.uplus.batch.domain.summary.service.ConsultationSummaryGenerator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/dummy")
@RequiredArgsConstructor
@Slf4j
public class DummyEventController {

    private final JdbcTemplate jdbcTemplate;
    private final ConsultationSummaryGenerator summaryGenerator;

    // 1. 분석 대기열 및 결과 테이블 초기화 (새로 추가된 기능)
    @PostMapping("/clear-analysis")
    public String clearAnalysisData() {
        log.info("[Dummy] 분석 관련 테이블(status, analysis) 초기화 시작...");

        try {
            // 외래키 체크 일시 해제
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0");

            // 요청하신 두 테이블만 초기화 (ID도 1부터 다시 시작)
            jdbcTemplate.execute("TRUNCATE TABLE retention_analysis");
            jdbcTemplate.execute("TRUNCATE TABLE result_event_status");

            // 외래키 체크 재설정
            jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1");

            log.info("[Dummy] 분석 테이블 초기화 완료");
            return "Successfully cleared: retention_analysis, result_event_status";
        } catch (Exception e) {
            log.error("[Dummy] 초기화 중 오류 발생: {}", e.getMessage());
            return "Error during clearing: " + e.getMessage();
        }
    }

    // 2. summary_event_status + result_event_status 동시 발행
    @PostMapping("/publish-events")
    public String publishEvents(
            @RequestParam Long startId,
            @RequestParam Long endId) {
        log.info("[Dummy] 이벤트 발행 시작: consultId {} ~ {}", startId, endId);
        try {
            int count = summaryGenerator.publishEvents(startId, endId);
            return String.format("Successfully published %d event(s) (REQUESTED)", count);
        } catch (Exception e) {
            log.error("[Dummy] 이벤트 발행 중 오류: {}", e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    // 3. 기존 이벤트 상태 대기열 생성 로직 (result_event_status 단독)
    @PostMapping("/event-status")
    public String generateEventStatus(
            @RequestParam Long startId,
            @RequestParam Long endId) {

        log.info("[Dummy] 이벤트 상태 대기열 생성 시작: consultId {} ~ {}", startId, endId);

        String sql = """
            INSERT INTO result_event_status (
                consult_id, 
                category_code, 
                status, 
                retry_count, 
                created_at, 
                updated_at
            )
            SELECT 
                consult_id, 
                category_code, 
                'REQUESTED', 
                0, 
                NOW(), 
                NOW()
            FROM consultation_results
            WHERE consult_id BETWEEN ? AND ?
            """;

        try {
            int affectedRows = jdbcTemplate.update(sql, startId, endId);
            log.info("[Dummy] {}건의 분석 요청 데이터 생성 완료", affectedRows);
            return String.format("Successfully created %d event status records (REQUESTED)", affectedRows);
        } catch (Exception e) {
            log.error("[Dummy] 데이터 생성 중 오류 발생: {}", e.getMessage());
            return "Error: " + e.getMessage();
        }
    }
}