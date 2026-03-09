package com.uplus.batch.jobs.monthly_report.config;

import com.uplus.batch.jobs.common.step.keyword.KeywordStatsTasklet;
import com.uplus.batch.jobs.monthly_report.step.admin.ChurnDefenseStatsTasklet;
import com.uplus.batch.jobs.monthly_report.step.admin.MonthlySubscriptionStatsTasklet;
import com.uplus.batch.jobs.monthly_report.step.customer_risk.MonthlyCustomerRiskTasklet;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class MonthlyReportConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final MonthlySubscriptionStatsTasklet monthlySubscriptionStatsTasklet;
  private final MonthlyCustomerRiskTasklet monthlyCustomerRiskTasklet;
  private final ChurnDefenseStatsTasklet churnDefenseStatsTasklet;
  private final MongoTemplate mongoTemplate;

  @Bean
  public Job monthlyAdminReportJob() {
    return new JobBuilder("monthlyAdminReportJob", jobRepository) //
        .start(monthlySubscriptionStatsStep())
        .next(monthlyKeywordStatsStep())
        .next(monthlyChurnDefenseStep())
        .next(monthlyCustomerRiskStep())
        .build();
  }

  @Bean
  public Step monthlySubscriptionStatsStep() {
    return new StepBuilder("monthlySubscriptionStatsStep", jobRepository) //
        .tasklet(monthlySubscriptionStatsTasklet, transactionManager)
        .build();
  }

  @Bean
  public Step monthlyKeywordStatsStep() {
    return new StepBuilder("monthlyKeywordStatsStep", jobRepository)
        .tasklet(new KeywordStatsTasklet(mongoTemplate, "monthly_report_snapshot"), transactionManager)
        .build();
  }

  @Bean
  public Step monthlyChurnDefenseStep() {
    return new StepBuilder("monthlyChurnDefenseStep", jobRepository)
        .tasklet(churnDefenseStatsTasklet, transactionManager)
        .build();
  }

  @Bean
  public Step monthlyCustomerRiskStep() {
    return new StepBuilder("monthlyCustomerRiskStep", jobRepository) //
        .tasklet(monthlyCustomerRiskTasklet, transactionManager)
        .build();
  }
}
