package com.uplus.batch.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 분석 배치 Job 수동 트리거 컨트롤러
 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class AnalysisJobController {

    private final JobLauncher jobLauncher;
    private final Job customerRiskJob;
    private final Job hourlyConsultJob;

    /**
     * 고객 특이사항 집계 Job 실행
     *
     * @param targetDate 집계 대상 날짜 (yyyy-MM-dd). 생략 시 어제.
     *
     * curl -X POST "http://localhost:8081/api/jobs/customer-risk?targetDate=2025-01-15"
     */
    @PostMapping("/customer-risk")
    public ResponseEntity<String> runCustomerRisk(
            @RequestParam(required = false) String targetDate
    ) throws Exception {

        JobParametersBuilder builder = new JobParametersBuilder()
                .addLong("runId", System.currentTimeMillis());

        if (targetDate != null && !targetDate.isBlank()) {
            builder.addString("targetDate", targetDate);
        }

        JobParameters params = builder.toJobParameters();

        log.info("[AnalysisJob] customerRiskJob 수동 실행 요청 — targetDate={}", targetDate);
        jobLauncher.run(customerRiskJob, params);

        return ResponseEntity.ok("CustomerRisk job started (targetDate=" + targetDate + ")");
    }

    /**
     * 시간대별 이슈 트렌드 집계 Job 실행
     *
     * @param targetDate 집계 대상 날짜 (yyyy-MM-dd). 생략 시 오늘.
     * @param slot       시간대 슬롯 (09-12, 12-15, 15-18). 생략 시 현재 시간 기준 직전 슬롯.
     *
     * curl -X POST "http://localhost:8081/api/jobs/hourly-consult?targetDate=2026-03-03&slot=09-12"
     */
    @PostMapping("/hourly-consult")
    public ResponseEntity<String> runHourlyConsult(
            @RequestParam(required = false) String targetDate,
            @RequestParam(required = false) String slot
    ) throws Exception {

        JobParametersBuilder builder = new JobParametersBuilder()
                .addLong("runId", System.currentTimeMillis());

        if (targetDate != null && !targetDate.isBlank()) {
            builder.addString("targetDate", targetDate);
        }
        if (slot != null && !slot.isBlank()) {
            builder.addString("slot", slot);
        }

        JobParameters params = builder.toJobParameters();

        log.info("[AnalysisJob] hourlyConsultJob 수동 실행 요청 — targetDate={}, slot={}", targetDate, slot);
        jobLauncher.run(hourlyConsultJob, params);

        return ResponseEntity.ok("HourlyConsult job started (targetDate=" + targetDate + ", slot=" + slot + ")");
    }
}