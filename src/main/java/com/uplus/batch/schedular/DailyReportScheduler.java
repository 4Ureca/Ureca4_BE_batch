package com.uplus.batch.schedular;

import java.time.LocalDate;
import java.time.LocalDateTime;
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
 * 일별 리포트 스케줄러 — 매일 새벽 2시 실행
 *
 * 실행 순서:
 *   1. 상담사 리포트 (dailyAgentReportJob)
 *   2. 관리자 리포트 (dailyAdminReportJob) × 3슬롯
 *
 * 상담사 리포트를 먼저 실행해야 관리자 리포트의 PerformanceTasklet이
 * agent snapshot에서 qualityScore를 참조할 수 있음.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DailyReportScheduler {

  private final JobLauncher jobLauncher;
  private final Job dailyAgentReportJob;
  private final Job dailyAdminReportJob;
  private final MongoTemplate mongoTemplate;

  private static final String[] SLOTS = {"09-12", "12-15", "15-18"};

  /** 크론 스케줄 — 어제 날짜 기준 */
  @Scheduled(cron = "0 0 2 * * *")
  public void runDailyBatch() {
    String date = LocalDate.now().minusDays(1).toString();
    runDailyBatch(date);
  }

  /** 특정 날짜 지정 실행 */
  public void runDailyBatch(String date) {
    // 1. 상담사 리포트
    try {
      jobLauncher.run(dailyAgentReportJob, new JobParametersBuilder()
          .addString("executionTime", LocalDateTime.now().toString())
          .addString("targetDate", date)
          .toJobParameters());
    } catch (Exception e) {
      log.error("[DailyScheduler] 상담사 리포트 실패 — {}: {}", date, e.getMessage());
    }

    // 2. 관리자 리포트 (슬롯별 3회)
    for (String slot : SLOTS) {
      try {
        jobLauncher.run(dailyAdminReportJob, new JobParametersBuilder()
            .addLong("runId", System.currentTimeMillis())
            .addString("startDate", date)
            .addString("endDate", date)
            .addString("targetCollection", "daily_report_snapshot")
            .addString("targetDate", date)
            .addString("slot", slot)
            .toJobParameters());
      } catch (Exception e) {
        log.error("[DailyScheduler] 관리자 리포트 실패 — {} slot={}: {}", date, slot, e.getMessage());
      }
    }
  }

  /** ALL 모드 — DB 내 모든 요약문 날짜 대상 실행 (비동기). count가 null이면 전체, 아니면 앞에서 N건만 */
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

    List<String> dates = results.getMappedResults().stream()
        .map(d -> d.getString("_id"))
        .collect(Collectors.toList());

    int total = dates.size();
    if (count != null && count > 0 && count < total) {
      dates = dates.subList(0, count);
    }
    int targetSize = dates.size();

    log.info("[DailyScheduler] ALL 모드 — 전체 {}건 중 {}건 실행, 비동기 시작", total, targetSize);

    List<String> targetDates = dates;
    new Thread(() -> {
      int success = 0;
      int fail = 0;
      for (int i = 0; i < targetDates.size(); i++) {
        String date = targetDates.get(i);
        try {
          runDailyBatch(date);
          success++;
          log.info("[DailyScheduler] ALL 진행 — {}/{} ({})", i + 1, targetDates.size(), date);
          Thread.sleep(500);  // 커넥션 풀 안정화
        } catch (Exception e) {
          fail++;
          log.error("[DailyScheduler] ALL 실패 — {}: {}", date, e.getMessage());
        }
      }
      log.info("[DailyScheduler] ALL 모드 완료 — 성공: {}, 실패: {}, 전체: {}", success, fail, targetDates.size());
    }, "daily-all-dates").start();
  }
}
