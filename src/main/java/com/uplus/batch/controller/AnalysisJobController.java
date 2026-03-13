package com.uplus.batch.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
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
    private final JdbcTemplate jdbcTemplate;
    private final MongoTemplate mongoTemplate;

    // ==================== 관리자 리포트 Job ====================
    private final Job dailyAdminReportJob;   // 일별 관리자 리포트 (performance + keyword + customerRisk + hourlyConsult)
    private final Job weeklyAdminReportJob;  // 주별 관리자 리포트 (performance + keyword + subscription)
    private final Job monthlyAdminReportJob; // 월별 관리자 리포트 (performance + keyword + subscription + churn + customerRisk)

    // ==================== 상담사 리포트 Job ====================
    private final Job dailyAgentReportJob;   // 일별 상담사 개인 리포트
    private final Job weeklyAgentReportJob;  // 주별 상담사 개인 리포트
    private final Job monthlyAgentReportJob; // 월별 상담사 개인 리포트


    // ==================== 데이터 현황 조회 ====================

    @Operation(summary = "📊 배치 실행 가능 데이터 현황",
        description = "결과서(consultation_results) + 원문(consultation_raw_texts) + 요약문(consultation_summary) 3개가 모두 존재하는 데이터만 조회합니다. 날짜 또는 상담사 ID로 검색할 수 있습니다.")
    @GetMapping("/data-status")
    public ResponseEntity<Map<String, Object>> getDataStatus(
            @Parameter(description = "검색 날짜 (생략 시 전체)", example = "2025-01-18") @RequestParam(required = false) String date,
            @Parameter(description = "상담사 ID (생략 시 전체)", example = "1") @RequestParam(required = false) Long empId
    ) {
        Map<String, Object> result = new LinkedHashMap<>();

        // 1. MySQL: 결과서 + 원문 + 상담사 + 권한 확인
        StringBuilder sqlBuilder = new StringBuilder("""
            SELECT cr.consult_id, DATE(cr.created_at) AS date,
                   e.emp_id, e.name AS agent_name,
                   jr.role_name
            FROM consultation_results cr
            INNER JOIN consultation_raw_texts crt ON cr.consult_id = crt.consult_id
            INNER JOIN employees e ON cr.emp_id = e.emp_id
            INNER JOIN employee_details ed ON e.emp_id = ed.emp_id
            INNER JOIN job_roles jr ON ed.job_role_id = jr.job_role_id
            WHERE 1=1
            """);
        if (date != null && !date.isBlank()) {
            sqlBuilder.append(" AND DATE(cr.created_at) = '").append(date).append("'");
        }
        if (empId != null) {
            sqlBuilder.append(" AND cr.emp_id = ").append(empId);
        }
        sqlBuilder.append(" ORDER BY cr.created_at, cr.consult_id");

        List<Map<String, Object>> mysqlRows = jdbcTemplate.queryForList(sqlBuilder.toString());
        Set<Long> mysqlConsultIds = mysqlRows.stream()
            .map(r -> ((Number) r.get("consult_id")).longValue())
            .collect(Collectors.toSet());

        // 2. MongoDB: 요약문 있는 consult_id 목록
        org.springframework.data.mongodb.core.query.Query mongoQuery =
            new org.springframework.data.mongodb.core.query.Query();
        if (!mysqlConsultIds.isEmpty()) {
            mongoQuery.addCriteria(Criteria.where("consultId").in(mysqlConsultIds));
        }
        mongoQuery.fields().include("consultId");
        List<Map> summaryDocs = mongoTemplate.find(mongoQuery, Map.class, "consultation_summary");
        Set<Long> summaryConsultIds = summaryDocs.stream()
            .map(d -> ((Number) d.get("consultId")).longValue())
            .collect(Collectors.toSet());

        // 3. 3개 모두 있는 것만 필터
        List<Map<String, Object>> readyList = mysqlRows.stream()
            .filter(r -> summaryConsultIds.contains(((Number) r.get("consult_id")).longValue()))
            .collect(Collectors.toList());

        // 4. 날짜별 요약
        Map<String, Long> dateSummary = readyList.stream()
            .collect(Collectors.groupingBy(
                r -> r.get("date").toString(),
                LinkedHashMap::new,
                Collectors.counting()
            ));

        result.put("total_ready", readyList.size());
        result.put("dates", dateSummary);
        result.put("details", readyList);

        return ResponseEntity.ok(result);
    }

    @Operation(summary = "📊 MongoDB 스냅샷 생성 현황 조회",
        description = "선택한 컬렉션의 스냅샷 현황을 조회합니다. " +
            "count: 전체 스냅샷 문서 수 | " +
            "dateRange: 가장 오래된 날짜 ~ 가장 최근 날짜 | " +
            "distinctDates: 데이터가 존재하는 고유 날짜 수")
    @GetMapping("/data-status/mongo")
    public ResponseEntity<Map<String, Object>> getMongoSnapshotStatus(
            @Parameter(description = "조회할 컬렉션",
                schema = @Schema(allowableValues = {
                    "daily_report_snapshot", "weekly_report_snapshot", "monthly_report_snapshot",
                    "daily_agent_report_snapshot", "weekly_agent_report_snapshot", "monthly_agent_report_snapshot"
                }))
            @RequestParam(required = false) String collection
    ) {
        Map<String, Object> status = new LinkedHashMap<>();

        String[] collections = collection != null && !collection.isBlank()
            ? new String[]{collection}
            : new String[]{
                "daily_report_snapshot", "weekly_report_snapshot", "monthly_report_snapshot",
                "daily_agent_report_snapshot", "weekly_agent_report_snapshot", "monthly_agent_report_snapshot"
            };

        for (String col : collections) {
            if (mongoTemplate.collectionExists(col)) {
                long count = mongoTemplate.count(
                    new org.springframework.data.mongodb.core.query.Query(), col);
                // 날짜 범위만 표시 (최소~최대)
                List<?> dates = mongoTemplate.findDistinct(
                    new org.springframework.data.mongodb.core.query.Query(),
                    "startAt", col, Object.class);
                Object minDate = dates.isEmpty() ? null : dates.get(0);
                Object maxDate = dates.isEmpty() ? null : dates.get(dates.size() - 1);
                status.put(col, Map.of(
                    "count", count,
                    "dateRange", minDate != null ? minDate + " ~ " + maxDate : "없음",
                    "distinctDates", dates.size()
                ));
            } else {
                status.put(col, Map.of("count", 0, "dateRange", "없음", "distinctDates", 0));
            }
        }

        return ResponseEntity.ok(status);
    }

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
