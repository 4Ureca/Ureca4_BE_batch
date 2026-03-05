package com.uplus.batch.schedular;

import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DailyReportScheduler {
  private final JobLauncher jobLauncher;
  private final Job dailyAgentReportJob;

  @Scheduled(cron = "0 0 2 * * *") // 매일 새벽 2시에 실행
  public void runDailyAgentReportJob() {
    try {
      JobParameters params = new JobParametersBuilder()
          .addString("datetime", LocalDateTime.now().toString())
          .toJobParameters();
      jobLauncher.run(dailyAgentReportJob, params);
    } catch (Exception e) {
      log.error("Batch Job Execution Failed: {}", e.getMessage());
    }
  }
}