package com.uplus.batch.schedular;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 월별 리포트 스케줄러 — 매월 1일 새벽 4시 실행
 *
 * 지난달 1일 ~ 말일 범위로 집계.
 *
 * 실행 순서:
 *   1. 상담사 리포트 (monthlyAgentReportJob)
 *   2. 관리자 리포트 (monthlyAdminReportJob)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MonthlyReportScheduler {

  private final JobLauncher jobLauncher;
  private final Job monthlyAgentReportJob;
  private final Job monthlyAdminReportJob;
  private final MongoTemplate mongoTemplate;

  /** 크론 스케줄 — 지난달 기준 */
  @Scheduled(cron = "0 0 4 1 * *")
  public void runMonthlyBatch() {
    LocalDate lastMonth = LocalDate.now().minusMonths(1);
    LocalDate firstDay = lastMonth.withDayOfMonth(1);
    LocalDate lastDay = lastMonth.withDayOfMonth(lastMonth.lengthOfMonth());
    runMonthlyBatch(firstDay.toString(), lastDay.toString());
  }

  /** 특정 날짜 기준 — 해당 월의 1일~말일 계산 */
  public void runMonthlyBatch(String date) {
    LocalDate target = LocalDate.parse(date);
    LocalDate firstDay = target.withDayOfMonth(1);
    LocalDate lastDay = target.withDayOfMonth(target.lengthOfMonth());
    runMonthlyBatch(firstDay.toString(), lastDay.toString());
  }

  /** 시작일~종료일 직접 지정 실행 */
  public void runMonthlyBatch(String startDate, String endDate) {
    // 1. 상담사 리포트
    try {
      jobLauncher.run(monthlyAgentReportJob, new JobParametersBuilder()
          .addString("executionTime", LocalDateTime.now().toString())
          .addString("jobType", "MONTHLY")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
    } catch (Exception e) {
      log.error("[MonthlyScheduler] 상담사 리포트 실패 — {} ~ {}: {}", startDate, endDate, e.getMessage());
    }

    // 2. 관리자 리포트
    try {
      jobLauncher.run(monthlyAdminReportJob, new JobParametersBuilder()
          .addLong("time", System.currentTimeMillis())
          .addString("targetCollection", "monthly_report_snapshot")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
    } catch (Exception e) {
      log.error("[MonthlyScheduler] 관리자 리포트 실패 — {} ~ {}: {}", startDate, endDate, e.getMessage());
    }
  }

  /** ALL 모드 — DB 내 모든 요약문 날짜에서 월 단위로 실행 (비동기). count가 null이면 전체, 아니면 앞에서 N건만 */
  public void runAllDates(Integer count) {
    Aggregation agg = Aggregation.newAggregation(
        Aggregation.project()
            .and(context -> new Document("$dateToString",
                new Document("format", "%Y-%m-%d")
                    .append("date", "$consultedAt")
                    .append("timezone", "+09:00")))
            .as("dateStr"),
        Aggregation.group("dateStr"),
        Aggregation.sort(org.springframework.data.domain.Sort.Direction.ASC, "_id")
    );

    AggregationResults<Document> results = mongoTemplate.aggregate(
        agg, "consultation_summary", Document.class);

    List<YearMonth> distinctMonths = results.getMappedResults().stream()
        .map(d -> LocalDate.parse(d.getString("_id")))
        .map(d -> YearMonth.from(d))
        .distinct()
        .sorted()
        .collect(Collectors.toList());

    int total = distinctMonths.size();
    if (count != null && count > 0 && count < total) {
      distinctMonths = distinctMonths.subList(0, count);
    }
    int targetSize = distinctMonths.size();

    log.info("[MonthlyScheduler] ALL 모드 — 전체 {}건 중 {}건 실행, 비동기 시작", total, targetSize);

    List<YearMonth> targetMonths = distinctMonths;
    new Thread(() -> {
      int success = 0;
      int fail = 0;
      for (int i = 0; i < targetMonths.size(); i++) {
        YearMonth ym = targetMonths.get(i);
        LocalDate firstDay = ym.atDay(1);
        LocalDate lastDay = ym.atEndOfMonth();
        try {
          runMonthlyBatch(firstDay.toString(), lastDay.toString());
          success++;
          log.info("[MonthlyScheduler] ALL 진행 — {}/{} ({})", i + 1, targetMonths.size(), ym);
          Thread.sleep(500);
        } catch (Exception e) {
          fail++;
          log.error("[MonthlyScheduler] ALL 실패 — {}: {}", ym, e.getMessage());
        }
      }
      log.info("[MonthlyScheduler] ALL 모드 완료 — 성공: {}, 실패: {}, 전체: {}개월", success, fail, targetMonths.size());
    }, "monthly-all-dates").start();
  }
}
