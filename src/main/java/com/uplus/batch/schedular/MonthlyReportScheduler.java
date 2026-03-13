package com.uplus.batch.schedular;

import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
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

  @Scheduled(cron = "0 0 4 1 * *")
  public void runMonthlyBatch() {
    LocalDate now = LocalDate.now();
    LocalDate lastMonth = now.minusMonths(1);
    LocalDate firstDay = lastMonth.withDayOfMonth(1);
    LocalDate lastDay = lastMonth.withDayOfMonth(lastMonth.lengthOfMonth());

    String startDate = firstDay.toString();
    String endDate = lastDay.toString();

    log.info("[MonthlyScheduler] 월별 배치 시작 — {} ~ {}", startDate, endDate);

    // 1. 상담사 리포트
    try {
      jobLauncher.run(monthlyAgentReportJob, new JobParametersBuilder()
          .addString("executionTime", LocalDateTime.now().toString())
          .addString("jobType", "MONTHLY")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
      log.info("[MonthlyScheduler] 상담사 리포트 완료 — {} ~ {}", startDate, endDate);
    } catch (Exception e) {
      log.error("[MonthlyScheduler] 상담사 리포트 실패: {}", e.getMessage());
    }

    // 2. 관리자 리포트
    try {
      jobLauncher.run(monthlyAdminReportJob, new JobParametersBuilder()
          .addLong("time", System.currentTimeMillis())
          .addString("targetCollection", "monthly_report_snapshot")
          .addString("startDate", startDate)
          .addString("endDate", endDate)
          .toJobParameters());
      log.info("[MonthlyScheduler] 관리자 리포트 완료 — {} ~ {}", startDate, endDate);
    } catch (Exception e) {
      log.error("[MonthlyScheduler] 관리자 리포트 실패: {}", e.getMessage());
    }

    log.info("[MonthlyScheduler] 월별 배치 완료 — {} ~ {}", startDate, endDate);
  }
}
