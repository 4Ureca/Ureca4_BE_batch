package com.uplus.batch.schedular;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
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
 * 주별 리포트 스케줄러 — 매주 월요일 새벽 3시 실행
 *
 * 지난주 월요일 ~ 일요일 범위로 집계.
 *
 * 실행 순서:
 *   1. 상담사 리포트 (weeklyAgentReportJob)
 *   2. 관리자 리포트 (weeklyAdminReportJob)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WeeklyReportScheduler {

  private final JobLauncher jobLauncher;
  private final Job weeklyAgentReportJob;
  private final Job weeklyAdminReportJob;
  private final MongoTemplate mongoTemplate;

  /** 크론 스케줄 — 지난주 기준 */
  @Scheduled(cron = "0 0 3 * * MON")
  public void runWeeklyBatch() {
    LocalDate now = LocalDate.now();
    LocalDate lastMonday = now.minusWeeks(1)
        .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
    LocalDate lastSunday = lastMonday.plusDays(6);
    runWeeklyBatch(lastMonday.toString(), lastSunday.toString());
  }

  /** 특정 날짜 기준 — 해당 주의 월~일 계산 */
  public void runWeeklyBatch(String date) {
    LocalDate target = LocalDate.parse(date);
    LocalDate monday = target.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
    LocalDate sunday = monday.plusDays(6);
    runWeeklyBatch(monday.toString(), sunday.toString());
  }

  /** 시작일~종료일 직접 지정 실행 */
  public void runWeeklyBatch(String startDate, String endDate) {
    // 1. 상담사 리포트
    try {
      jobLauncher.run(weeklyAgentReportJob, new JobParametersBuilder()
          .addString("executionTime", LocalDateTime.now().toString())
          .addString("jobType", "WEEKLY")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
    } catch (Exception e) {
      log.error("[WeeklyScheduler] 상담사 리포트 실패 — {} ~ {}: {}", startDate, endDate, e.getMessage());
    }

    // 2. 관리자 리포트
    try {
      jobLauncher.run(weeklyAdminReportJob, new JobParametersBuilder()
          .addLong("time", System.currentTimeMillis())
          .addString("targetCollection", "weekly_report_snapshot")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
    } catch (Exception e) {
      log.error("[WeeklyScheduler] 관리자 리포트 실패 — {} ~ {}: {}", startDate, endDate, e.getMessage());
    }
  }

  /** ALL 모드 — DB 내 모든 요약문 날짜에서 주 단위로 실행 (비동기). count가 null이면 전체, 아니면 앞에서 N건만 */
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

    List<LocalDate> distinctWeeks = results.getMappedResults().stream()
        .map(d -> LocalDate.parse(d.getString("_id")))
        .map(d -> d.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)))
        .distinct()
        .sorted()
        .collect(Collectors.toList());

    int total = distinctWeeks.size();
    if (count != null && count > 0 && count < total) {
      distinctWeeks = distinctWeeks.subList(0, count);
    }
    int targetSize = distinctWeeks.size();

    log.info("[WeeklyScheduler] ALL 모드 — 전체 {}건 중 {}건 실행, 비동기 시작", total, targetSize);

    List<LocalDate> targetWeeks = distinctWeeks;
    new Thread(() -> {
      int success = 0;
      int fail = 0;
      for (int i = 0; i < targetWeeks.size(); i++) {
        LocalDate monday = targetWeeks.get(i);
        LocalDate sunday = monday.plusDays(6);
        try {
          runWeeklyBatch(monday.toString(), sunday.toString());
          success++;
          log.info("[WeeklyScheduler] ALL 진행 — {}/{} ({} ~ {})", i + 1, targetWeeks.size(), monday, sunday);
          Thread.sleep(500);
        } catch (Exception e) {
          fail++;
          log.error("[WeeklyScheduler] ALL 실패 — {} ~ {}: {}", monday, sunday, e.getMessage());
        }
      }
      log.info("[WeeklyScheduler] ALL 모드 완료 — 성공: {}, 실패: {}, 전체: {}주", success, fail, targetWeeks.size());
    }, "weekly-all-dates").start();
  }
}
