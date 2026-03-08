package com.uplus.batch.jobs.weekly_agent_report;

import com.uplus.batch.jobs.weekly_agent_report.entity.WeeklyAgentReportSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class WeeklyAgentReportJobConfig {

  private final MongoTemplate mongoTemplate;
  private final WeeklyAgentReportProcessor weeklyAgentReportProcessor;
  private final PlatformTransactionManager transactionManager;

  @Bean
  public Job weeklyAgentReportJob(JobRepository jobRepository, Step weeklyAgentReportStep) {
    return new JobBuilder("weeklyAgentReportJob", jobRepository)
        .start(weeklyAgentReportStep)
        .build();
  }

  @Bean
  public Step weeklyAgentReportStep(JobRepository jobRepository) {
    return new StepBuilder("weeklyAgentReportStep", jobRepository)
        .<Long, WeeklyAgentReportSnapshot>chunk(10, transactionManager) // 상담사 10명씩 처리. 나중에 수정
        .reader(weeklyAgentIdReader())
        .processor(weeklyAgentReportProcessor)
        .writer(weeklySnapshotWriter())
        .build();
  }

  @Bean
  @StepScope
  public ItemReader<Long> weeklyAgentIdReader() {
    List<Long> agentIds = mongoTemplate.getCollection("consultation_summary")
        .distinct("agent._id", Long.class)
        .into(new ArrayList<Long>())
        .stream()
        .collect(Collectors.toList());

    return new ListItemReader<>(agentIds);
  }

  @Bean
  public ItemWriter<WeeklyAgentReportSnapshot> weeklySnapshotWriter() {
    // 주별 리포트 전용 컬렉션에 저장함
    return items -> items.forEach(item -> mongoTemplate.save(item, "weekly_agent_report_snapshot"));
  }
}