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
}