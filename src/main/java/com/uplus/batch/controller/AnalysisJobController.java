package com.uplus.batch.controller;

import java.time.LocalDateTime;
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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 분석 배치 Job 수동 트리거 컨트롤러
 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class AnalysisJobController {

    private final JobLauncher jobLauncher;

    // ==================== 관리자 리포트 Job ====================
    private final Job dailyAdminReportJob;   // 일별 관리자 리포트 (performance + keyword + customerRisk + hourlyConsult)
    private final Job weeklyAdminReportJob;  // 주별 관리자 리포트 (performance + keyword + subscription)
    private final Job monthlyAdminReportJob; // 월별 관리자 리포트 (performance + keyword + subscription + churn + customerRisk)

    // ==================== 상담사 리포트 Job ====================
    private final Job dailyAgentReportJob;   // 일별 상담사 개인 리포트
    private final Job weeklyAgentReportJob;  // 주별 상담사 개인 리포트
    private final Job monthlyAgentReportJob; // 월별 상담사 개인 리포트


    // ==================== 관리자 리포트 엔드포인트 ====================

    /**
     * 일별 관리자 리포트 통합 배치 실행
     * Step: performance → keyword → customerRisk → hourlyConsult
     *
     * @param date 집계 대상 날짜 (yyyy-MM-dd)
     * @param slot 시간대 슬롯 (09-12, 12-15, 15-18). 생략 시 현재 시간 기준.
     *
     * curl -X POST "http://localhost:8081/api/jobs/run-daily-batch?date=2025-01-18"
     * curl -X POST "http://localhost:8081/api/jobs/run-daily-batch?date=2025-01-18&slot=09-12"
     */
    @PostMapping("/run-daily-batch")
    public ResponseEntity<String> runDailyBatch(
            @RequestParam String date,
            @RequestParam(required = false) String slot
    ) {
        try {
            JobParametersBuilder builder = new JobParametersBuilder()
                    .addLong("runId", System.currentTimeMillis())
                    .addString("startDate", date)
                    .addString("endDate", date)
                    .addString("targetCollection", "daily_report_snapshot")
                    .addString("targetDate", date);

            if (slot != null && !slot.isBlank()) {
                builder.addString("slot", slot);
            }

            JobParameters params = builder.toJobParameters();

            log.info("[AnalysisJob] dailyAdminReportJob 수동 실행 요청 — date={}, slot={}", date, slot);
            jobLauncher.run(dailyAdminReportJob, params);

            return ResponseEntity.ok("DailyAdminReport job started (date=" + date + ", slot=" + slot + ")");
        } catch (Exception e) {
            log.error("일별 관리자 리포트 배치 실행 중 오류 발생: ", e);
            return ResponseEntity.internalServerError().body("배치 실행 실패: " + e.getMessage());
        }
    }

    /**
     * 주별 관리자 리포트 통합 배치 실행
     * Step: performance → keyword → subscription
     *
     * curl -X GET "http://localhost:8081/api/jobs/run-weekly-batch?startDate=2025-01-13&endDate=2025-01-19"
     */
    @GetMapping("/run-weekly-batch")
    public ResponseEntity<String> runWeeklyBatch(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate
    ) {
        try {
            JobParametersBuilder builder = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString("targetCollection", "weekly_report_snapshot");

            if (startDate != null && !startDate.isBlank()) {
                builder.addString("startDate", startDate);
            }
            if (endDate != null && !endDate.isBlank()) {
                builder.addString("endDate", endDate);
            }

            JobParameters params = builder.toJobParameters();

            log.info("[AnalysisJob] weeklyAdminReportJob 수동 실행 요청 — startDate={}, endDate={}", startDate, endDate);
            jobLauncher.run(weeklyAdminReportJob, params);
            return ResponseEntity.ok("주별 배치 실행 성공! (startDate=" + startDate + ", endDate=" + endDate + ")");
        } catch (Exception e) {
            log.error("주별 배치 실행 중 오류 발생: ", e);
            return ResponseEntity.internalServerError().body("배치 실행 실패: " + e.getMessage());
        }
    }

    /**
     * 월별 관리자 리포트 통합 배치 실행
     * Step: performance → keyword → subscription → churnDefense → customerRisk
     *
     * curl -X GET "http://localhost:8081/api/jobs/run-monthly-batch?startDate=2025-01-01&endDate=2025-01-31"
     */
    @GetMapping("/run-monthly-batch")
    public ResponseEntity<String> runMonthlyBatch(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate
    ) {
        try {
            JobParametersBuilder builder = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString("targetCollection", "monthly_report_snapshot");

            if (startDate != null && !startDate.isBlank()) {
                builder.addString("startDate", startDate);
            }
            if (endDate != null && !endDate.isBlank()) {
                builder.addString("endDate", endDate);
            }

            JobParameters params = builder.toJobParameters();

            log.info("[AnalysisJob] monthlyAdminReportJob 수동 실행 요청 — startDate={}, endDate={}", startDate, endDate);
            jobLauncher.run(monthlyAdminReportJob, params);
            return ResponseEntity.ok("월별 배치 실행 성공! (startDate=" + startDate + ", endDate=" + endDate + ")");
        } catch (Exception e) {
            log.error("월별 배치 실행 중 오류 발생: ", e);
            return ResponseEntity.internalServerError().body("배치 실행 실패: " + e.getMessage());
        }
    }

    // ==================== 상담사 리포트 엔드포인트 ====================

    /**
     * 상담사 일별 리포트 배치 수동 실행
     * 호출: http://localhost:8081/api/jobs/daily-agent-report
     */
    @GetMapping("/daily-agent-report")
    public String runDailyAgentReportJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                .addString("executionTime", LocalDateTime.now().toString())
                .toJobParameters();

            jobLauncher.run(dailyAgentReportJob, jobParameters);

            return "Daily Agent Report Batch has been started for yesterday's data.";
        } catch (Exception e) {
            log.error("Batch Manual Execution Failed", e);
            return "Batch Job Failed: " + e.getMessage();
        }
    }

    /**
     * 상담사 주별 리포트 배치 수동 실행
     * 호출: http://localhost:8081/api/jobs/weekly-agent-report
     */
    @GetMapping("/weekly-agent-report")
    public String runWeeklyAgentReportJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                .addString("executionTime", LocalDateTime.now().toString())
                .addString("jobType", "WEEKLY")
                .toJobParameters();

            jobLauncher.run(weeklyAgentReportJob, jobParameters);

            return "Weekly Agent Report Batch has been started (Based on last week's Daily Snapshots).";
        } catch (Exception e) {
            log.error("Weekly Batch Manual Execution Failed", e);
            return "Weekly Batch Job Failed: " + e.getMessage();
        }
    }

    /**
     * 상담사 월별 리포트 배치 수동 실행
     * 호출: http://localhost:8081/api/jobs/monthly-agent-report
     */
    @GetMapping("/monthly-agent-report")
    public String runMonthlyAgentReportJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                .addString("executionTime", LocalDateTime.now().toString())
                .addString("jobType", "MONTHLY")
                .toJobParameters();

            jobLauncher.run(monthlyAgentReportJob, jobParameters);

            return "Monthly Agent Report Batch has been started (Based on last month's Daily Snapshots).";
        } catch (Exception e) {
            log.error("Monthly Batch Manual Execution Failed", e);
            return "Monthly Batch Job Failed: " + e.getMessage();
        }
    }
}
