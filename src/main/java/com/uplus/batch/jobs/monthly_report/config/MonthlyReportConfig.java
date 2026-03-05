package com.uplus.batch.jobs.monthly_report.config;

import com.uplus.batch.jobs.monthly_report.step.admin.MonthlySubscriptionStatsTasklet;
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
public class MonthlyReportConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final MonthlySubscriptionStatsTasklet monthlySubscriptionStatsTasklet;

  @Bean
  public Job monthlyAdminReportJob() {
    return new JobBuilder("monthlyAdminReportJob", jobRepository) //
        .start(monthlySubscriptionStatsStep())
        .build();
  }

  @Bean
  public Step monthlySubscriptionStatsStep() {
    return new StepBuilder("monthlySubscriptionStatsStep", jobRepository) //
        .tasklet(monthlySubscriptionStatsTasklet, transactionManager) // transactionManager 추가 필요
        .build();
  }

}
