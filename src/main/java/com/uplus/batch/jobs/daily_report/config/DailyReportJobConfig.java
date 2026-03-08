package com.uplus.batch.jobs.daily_report.config;

import com.uplus.batch.jobs.weekly_report.step.performance.PerformanceTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class DailyReportJobConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;

  @Bean
  public Job dailyPerformanceJob(Step dailyPerformanceStep) {
    return new JobBuilder("dailyPerformanceJob", jobRepository)
        .start(dailyPerformanceStep)
        .build();
  }

  @Bean
  public Step dailyPerformanceStep(PerformanceTasklet performanceTasklet) {
    return new StepBuilder("dailyPerformanceStep", jobRepository)
        .tasklet(performanceTasklet, transactionManager)
        .build();
  }
}