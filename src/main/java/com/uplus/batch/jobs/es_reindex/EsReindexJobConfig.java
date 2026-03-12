package com.uplus.batch.jobs.es_reindex;

import com.uplus.batch.domain.summary.entity.ConsultationSummary;
import com.uplus.batch.jobs.es_reindex.chunk.EsReindexItemReader;
import com.uplus.batch.jobs.es_reindex.chunk.EsReindexItemWriter;
import com.uplus.batch.domain.summary.service.SummaryProcessingLockService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.data.mongodb.core.MongoTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class EsReindexJobConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;
  private final JobLauncher jobLauncher;
  private final EsReindexItemWriter writer;
  private final MongoTemplate mongoTemplate;
  private final SummaryProcessingLockService lockService;

  @Bean
  @StepScope
  public EsReindexItemReader esReindexItemReader() {
    return new EsReindexItemReader(mongoTemplate, lockService);
  }

  @Bean
  public Job esReindexJob() {
    return new JobBuilder("esReindexJob", jobRepository)
        .start(esReindexStep())
        .build();
  }

  @Bean
  public Step esReindexStep() {
    return new StepBuilder("esReindexStep", jobRepository)
        .<ConsultationSummary, ConsultationSummary>chunk(100, transactionManager)
        .reader(esReindexItemReader())
        .writer(writer)
        .build();
  }

  @Scheduled(fixedDelay = 10 * 60 * 1000)
  public void schedule() {
    try {
      JobParameters params = new JobParametersBuilder()
          .addLong("runAt", System.currentTimeMillis())
          .toJobParameters();

      jobLauncher.run(esReindexJob(), params);

    } catch (Exception e) {
      log.error("ES 재처리 배치 실행 실패", e);
    }
  }
}