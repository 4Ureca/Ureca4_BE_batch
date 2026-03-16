package com.uplus.batch.schedular;

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
public class EsReindexScheduler {

  private final JobLauncher jobLauncher;
  private final Job esReindexJob;

  @Scheduled(fixedDelay = 10 * 60 * 1000)
  public void schedule() {
    try {
      JobParameters params = new JobParametersBuilder()
          .addLong("runAt", System.currentTimeMillis())
          .toJobParameters();

      jobLauncher.run(esReindexJob, params);

    } catch (Exception e) {
      log.error("ES 재처리 배치 실행 실패", e);
    }
  }
}
