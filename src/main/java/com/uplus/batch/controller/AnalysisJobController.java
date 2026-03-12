package com.uplus.batch.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "배치 수동 실행", description = "관리자/상담사 리포트 배치를 수동으로 실행합니다")
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

    @Operation(summary = "② 일별 관리자 리포트 배치 (슬롯별 3회)", description = "performance → keyword → customerRisk → hourlyConsult 순서로 실행")
    @PostMapping("/run-daily-batch")
    public ResponseEntity<String> runDailyBatch(
            @Parameter(description = "집계 대상 날짜", example = "2025-01-18") @RequestParam String date,
            @Parameter(description = "시간대 슬롯. 생략 시 현재 시간 기준", schema = @Schema(allowableValues = {"09-12", "12-15", "15-18"})) @RequestParam(required = false) String slot
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

    @Operation(summary = "④ 주별 관리자 리포트 배치", description = "performance → keyword → subscription 순서로 실행")
    @GetMapping("/run-weekly-batch")
    public ResponseEntity<String> runWeeklyBatch(
            @Parameter(description = "시작일 (월요일)", example = "2025-01-13") @RequestParam(required = false) String startDate,
            @Parameter(description = "종료일 (일요일)", example = "2025-01-19") @RequestParam(required = false) String endDate
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

    @Operation(summary = "⑥ 월별 관리자 리포트 배치", description = "performance → keyword → subscription → churnDefense → customerRisk 순서로 실행")
    @GetMapping("/run-monthly-batch")
    public ResponseEntity<String> runMonthlyBatch(
            @Parameter(description = "시작일 (1일)", example = "2025-01-01") @RequestParam(required = false) String startDate,
            @Parameter(description = "종료일 (말일)", example = "2025-01-31") @RequestParam(required = false) String endDate
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

    @Operation(summary = "① 일별 상담사 리포트 배치", description = "상담사별 일별 개인 리포트 생성")
    @GetMapping("/daily-agent-report")
    public String runDailyAgentReportJob(
            @Parameter(description = "집계 대상 날짜 (생략 시 어제)", example = "2025-01-18") @RequestParam(required = false) String date
    ) {
        try {
            JobParametersBuilder builder = new JobParametersBuilder()
                .addString("executionTime", LocalDateTime.now().toString());

            if (date != null && !date.isBlank()) {
                builder.addString("targetDate", date);
            }

            jobLauncher.run(dailyAgentReportJob, builder.toJobParameters());

            return "Daily Agent Report Batch has been started (date=" + date + ")";
        } catch (Exception e) {
            log.error("Batch Manual Execution Failed", e);
            return "Batch Job Failed: " + e.getMessage();
        }
    }

    @Operation(summary = "③ 주별 상담사 리포트 배치", description = "상담사별 주별 개인 리포트 생성 (생략 시 지난주)")
    @GetMapping("/weekly-agent-report")
    public String runWeeklyAgentReportJob(
            @Parameter(description = "시작일 (월요일)", example = "2025-01-13") @RequestParam(required = false) String startDate,
            @Parameter(description = "종료일 (일요일)", example = "2025-01-19") @RequestParam(required = false) String endDate
    ) {
        try {
            JobParametersBuilder builder = new JobParametersBuilder()
                .addString("executionTime", LocalDateTime.now().toString())
                .addString("jobType", "WEEKLY");

            if (startDate != null && !startDate.isBlank()) {
                builder.addString("startDate", startDate);
            }
            if (endDate != null && !endDate.isBlank()) {
                builder.addString("endDate", endDate);
            }

            jobLauncher.run(weeklyAgentReportJob, builder.toJobParameters());

            return "Weekly Agent Report Batch has been started (startDate=" + startDate + ", endDate=" + endDate + ")";
        } catch (Exception e) {
            log.error("Weekly Batch Manual Execution Failed", e);
            return "Weekly Batch Job Failed: " + e.getMessage();
        }
    }

    @Operation(summary = "⑤ 월별 상담사 리포트 배치", description = "상담사별 월별 개인 리포트 생성 (생략 시 지난달)")
    @GetMapping("/monthly-agent-report")
    public String runMonthlyAgentReportJob(
            @Parameter(description = "시작일 (1일)", example = "2025-01-01") @RequestParam(required = false) String startDate,
            @Parameter(description = "종료일 (말일)", example = "2025-01-31") @RequestParam(required = false) String endDate
    ) {
        try {
            JobParametersBuilder builder = new JobParametersBuilder()
                .addString("executionTime", LocalDateTime.now().toString())
                .addString("jobType", "MONTHLY");

            if (startDate != null && !startDate.isBlank()) {
                builder.addString("startDate", startDate);
            }
            if (endDate != null && !endDate.isBlank()) {
                builder.addString("endDate", endDate);
            }

            jobLauncher.run(monthlyAgentReportJob, builder.toJobParameters());

            return "Monthly Agent Report Batch has been started (startDate=" + startDate + ", endDate=" + endDate + ")";
        } catch (Exception e) {
            log.error("Monthly Batch Manual Execution Failed", e);
            return "Monthly Batch Job Failed: " + e.getMessage();
        }
    }
}
