package com.uplus.batch.jobs.summary_sync.config;

import com.uplus.batch.domain.summary.dto.SummaryEventStatusRow;
import com.uplus.batch.jobs.summary_sync.chunk.SummarySyncItemProcessor;
import com.uplus.batch.jobs.summary_sync.chunk.SummarySyncItemReader;
import com.uplus.batch.jobs.summary_sync.chunk.SummarySyncItemWriter;
import lombok.RequiredArgsConstructor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;

import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;

import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class SummarySyncJobConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;

  private final SummarySyncItemReader summarySyncItemReader;
  private final SummarySyncItemProcessor summarySyncItemProcessor;
  private final SummarySyncItemWriter summarySyncItemWriter;

  @Bean
  public Job summarySyncJob(Step summarySyncStep) {

    return new JobBuilder("summarySyncJob", jobRepository)
        .incrementer(new RunIdIncrementer())
        .start(summarySyncStep)
        .build();
  }

  @Bean
  public Step summarySyncStep(
      @Value("${app.summary-sync.batch-size:500}") int chunkSize
  ) {

    return new StepBuilder("summarySyncStep", jobRepository)
        .<SummaryEventStatusRow, SummaryEventStatusRow>chunk(chunkSize, transactionManager)
        .reader(summarySyncItemReader)
        .processor(summarySyncItemProcessor)
        .writer(summarySyncItemWriter)
        .build();
  }
}