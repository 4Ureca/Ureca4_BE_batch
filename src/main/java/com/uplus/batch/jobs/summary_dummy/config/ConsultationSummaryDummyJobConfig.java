package com.uplus.batch.jobs.summary_dummy.config;

import com.uplus.batch.jobs.summary_dummy.tasklet.ConsultationSummaryDummyTasklet;
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
public class ConsultationSummaryDummyJobConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final ConsultationSummaryDummyTasklet tasklet;

  @Bean
  public Job consultationSummaryDummyJob() {
    return new JobBuilder("consultationSummaryDummyJob", jobRepository)
        .start(consultationSummaryDummyStep())
        .build();
  }

  @Bean
  public Step consultationSummaryDummyStep() {
    return new StepBuilder("consultationSummaryDummyStep", jobRepository)
        .tasklet(tasklet, transactionManager)
        .build();
  }
}