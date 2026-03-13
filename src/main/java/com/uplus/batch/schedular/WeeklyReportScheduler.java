package com.uplus.batch.schedular;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
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

  @Scheduled(cron = "0 0 3 * * MON")
  public void runWeeklyBatch() {
    LocalDate now = LocalDate.now();
    LocalDate lastMonday = now.minusWeeks(1)
        .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
    LocalDate lastSunday = lastMonday.plusDays(6);

    String startDate = lastMonday.toString();
    String endDate = lastSunday.toString();

    log.info("[WeeklyScheduler] 주별 배치 시작 — {} ~ {}", startDate, endDate);

    // 1. 상담사 리포트
    try {
      jobLauncher.run(weeklyAgentReportJob, new JobParametersBuilder()
          .addString("executionTime", LocalDateTime.now().toString())
          .addString("jobType", "WEEKLY")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
      log.info("[WeeklyScheduler] 상담사 리포트 완료 — {} ~ {}", startDate, endDate);
    } catch (Exception e) {
      log.error("[WeeklyScheduler] 상담사 리포트 실패: {}", e.getMessage());
    }

    // 2. 관리자 리포트
    try {
      jobLauncher.run(weeklyAdminReportJob, new JobParametersBuilder()
          .addLong("time", System.currentTimeMillis())
          .addString("targetCollection", "weekly_report_snapshot")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
      log.info("[WeeklyScheduler] 관리자 리포트 완료 — {} ~ {}", startDate, endDate);
    } catch (Exception e) {
      log.error("[WeeklyScheduler] 관리자 리포트 실패: {}", e.getMessage());
    }

    log.info("[WeeklyScheduler] 주별 배치 완료 — {} ~ {}", startDate, endDate);
  }
}
