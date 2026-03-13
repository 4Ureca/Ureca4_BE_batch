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

  private static final String[] SLOTS = {"09-12", "12-15", "15-18"};

  @Scheduled(cron = "0 0 2 * * *")
  public void runDailyBatch() {
    LocalDate yesterday = LocalDate.now().minusDays(1);
    String date = yesterday.toString();

    log.info("[DailyScheduler] 일별 배치 시작 — targetDate={}", date);

    // 1. 상담사 리포트
    try {
      jobLauncher.run(dailyAgentReportJob, new JobParametersBuilder()
          .addString("executionTime", LocalDateTime.now().toString())
          .addString("targetDate", date)
          .toJobParameters());
      log.info("[DailyScheduler] 상담사 리포트 완료 — {}", date);
    } catch (Exception e) {
      log.error("[DailyScheduler] 상담사 리포트 실패: {}", e.getMessage());
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
        log.info("[DailyScheduler] 관리자 리포트 완료 — {} slot={}", date, slot);
      } catch (Exception e) {
        log.error("[DailyScheduler] 관리자 리포트 실패 (slot={}): {}", slot, e.getMessage());
      }
    }

    log.info("[DailyScheduler] 일별 배치 완료 — {}", date);
  }
}
