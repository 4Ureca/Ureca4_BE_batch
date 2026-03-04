package com.uplus.batch.jobs.daily_report.step.keyword_rank;

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
public class DailyKeywordJobConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final KeywordRankTasklet keywordRankTasklet;

  @Bean
  public Job dailyKeywordJob() {
    return new JobBuilder("dailyKeywordJob", jobRepository)
        .start(dailyKeywordStep())
        .build();
  }

  @Bean
  public Step dailyKeywordStep() {
    return new StepBuilder("dailyKeywordStep", jobRepository)
        .tasklet(keywordRankTasklet, transactionManager)
        .build();
  }
}
