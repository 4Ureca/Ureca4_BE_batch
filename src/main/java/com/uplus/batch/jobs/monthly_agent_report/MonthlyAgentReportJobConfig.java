package com.uplus.batch.jobs.monthly_agent_report;

import com.uplus.batch.jobs.monthly_agent_report.entity.MonthlyAgentReportSnapshot;
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
public class MonthlyAgentReportJobConfig {

  private final MongoTemplate mongoTemplate;
  private final MonthlyAgentReportProcessor monthlyAgentReportProcessor;
  private final PlatformTransactionManager transactionManager;

  @Bean
  public Job monthlyAgentReportJob(JobRepository jobRepository, Step monthlyAgentReportStep) {
    return new JobBuilder("monthlyAgentReportJob", jobRepository)
        .start(monthlyAgentReportStep)
        .build();
  }

  @Bean
  public Step monthlyAgentReportStep(JobRepository jobRepository) {
    return new StepBuilder("monthlyAgentReportStep", jobRepository)
        .<Long, MonthlyAgentReportSnapshot>chunk(10, transactionManager)
        .reader(monthlyAgentIdReader())
        .processor(monthlyAgentReportProcessor)
        .writer(monthlySnapshotWriter())
        .build();
  }

//  @Bean
//  @StepScope
//  public ItemReader<String> monthlyAgentIdReader() {
//    // 일별 스냅샷 컬렉션에서 고유한 상담사 ID들을 가져옴
//    List<String> agentIds = mongoTemplate.getCollection("daily_agent_report_snapshot")
//        .distinct("agentId", String.class)
//        .into(new ArrayList<>());
//    return new ListItemReader<>(agentIds);
//  }

  @Bean
  @StepScope
  public ItemReader<Long> monthlyAgentIdReader() { // 1. 반환 타입을 Long으로 변경
    List<Long> agentIds = mongoTemplate.getCollection("consultation_summary")
        .distinct("agent._id", Long.class) // 2. DB에서 Long 타입으로 추출
        .into(new ArrayList<Long>())
        .stream()
        .collect(Collectors.toList()); // 3. String 변환 로직 삭제

    return new ListItemReader<>(agentIds);
  }

  @Bean
  public ItemWriter<MonthlyAgentReportSnapshot> monthlySnapshotWriter() {
    // 월별 전용 컬렉션에 저장
    return items -> items.forEach(item -> mongoTemplate.save(item, "monthly_agent_report_snapshot"));
  }
}