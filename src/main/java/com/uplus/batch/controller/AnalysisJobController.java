package com.uplus.batch.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    private final Job weeklyPerformanceJob;
    private final Job monthlyPerformanceJob;

    private final Job weeklyAdminReportJob; // 주별 전체 리포트 배치
    private final Job monthlyAdminReportJob; // 월별 리포트 Job 추가


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
     * 주간 전체 상담 성과 집계 Job 실행
     *
     * @param startDate 집계 시작 날짜 (yyyy-MM-dd, 월요일)
     * @param endDate   집계 종료 날짜 (yyyy-MM-dd, 일요일)
     *
     * curl -X POST "http://localhost:8081/api/jobs/weekly-performance?startDate=2025-01-13&endDate=2025-01-19"
     */
    @PostMapping("/weekly-performance")
    public ResponseEntity<String> runWeeklyPerformance(
            @RequestParam String startDate,
            @RequestParam String endDate
    ) throws Exception {

        JobParameters params = new JobParametersBuilder()
                .addLong("runId", System.currentTimeMillis())
                .addString("startDate", startDate)
                .addString("endDate", endDate)
                .addString("targetCollection", "weekly_report_snapshot")
                .toJobParameters();

        log.info("[AnalysisJob] weeklyPerformanceJob 수동 실행 요청 — {} ~ {}", startDate, endDate);
        jobLauncher.run(weeklyPerformanceJob, params);

        return ResponseEntity.ok("WeeklyPerformance job started (" + startDate + " ~ " + endDate + ")");
    }

    /**
     * 월간 전체 상담 성과 집계 Job 실행
     *
     * @param startDate 집계 시작 날짜 (yyyy-MM-dd, 1일)
     * @param endDate   집계 종료 날짜 (yyyy-MM-dd, 말일)
     *
     * curl -X POST "http://localhost:8081/api/jobs/monthly-performance?startDate=2025-01-01&endDate=2025-01-31"
     */
    @PostMapping("/monthly-performance")
    public ResponseEntity<String> runMonthlyPerformance(
            @RequestParam String startDate,
            @RequestParam String endDate
    ) throws Exception {

        JobParameters params = new JobParametersBuilder()
                .addLong("runId", System.currentTimeMillis())
                .addString("startDate", startDate)
                .addString("endDate", endDate)
                .addString("targetCollection", "monthly_report_snapshot")
                .toJobParameters();

        log.info("[AnalysisJob] monthlyPerformanceJob 수동 실행 요청 — {} ~ {}", startDate, endDate);
        jobLauncher.run(monthlyPerformanceJob, params);

        return ResponseEntity.ok("MonthlyPerformance job started (" + startDate + " ~ " + endDate + ")");
    }


    /**
     * 주별 관리자 리포트 수동 실행
     * curl -X GET "http://localhost:8081/api/jobs/run-weekly-batch"
     */
    @GetMapping("/run-weekly-batch")
    public ResponseEntity<String> runWeeklyBatch() {
        try {
            JobParameters params = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

            log.info("[AnalysisJob] weeklyAdminReportJob 수동 실행 요청");
            jobLauncher.run(weeklyAdminReportJob, params);
            return ResponseEntity.ok("주별 배치 실행 성공!");
        } catch (Exception e) {
            log.error("주별 배치 실행 중 오류 발생: ", e);
            return ResponseEntity.internalServerError().body("배치 실행 실패: " + e.getMessage());
        }
    }


    /**
     * 월별 관리자 리포트 수동 실행
     * curl -X GET "http://localhost:8081/api/jobs/run-monthly-batch"
     */
    @GetMapping("/run-monthly-batch")
    public ResponseEntity<String> runMonthlyBatch() {
        try {
            JobParameters params = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

            log.info("[AnalysisJob] monthlyAdminReportJob 수동 실행 요청");
            jobLauncher.run(monthlyAdminReportJob, params);
            return ResponseEntity.ok("월별 배치 실행 성공!");
        } catch (Exception e) {
            log.error("월별 배치 실행 중 오류 발생: ", e);
            return ResponseEntity.internalServerError().body("배치 실행 실패: " + e.getMessage());
        }
    }
}
