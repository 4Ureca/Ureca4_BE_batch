package com.uplus.batch.schedular;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SummarySyncScheduler {

  private final JobLauncher jobLauncher;

  @Qualifier("summarySyncJob")
  private final Job summarySyncJob;

  @Scheduled(cron = "${app.summary-sync.cron:0 */5 * * * *}")
  public void syncRequestedConsultationSummary() {
    try {
      JobParameters params = new JobParametersBuilder()
          .addLong("runAt", System.currentTimeMillis())
          .toJobParameters();

      jobLauncher.run(summarySyncJob, params);
    } catch (Exception e) {
      log.error("Failed to launch summarySyncJob", e);
    }
  }
}
