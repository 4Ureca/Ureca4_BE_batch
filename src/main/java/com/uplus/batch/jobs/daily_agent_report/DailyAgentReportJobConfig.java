package com.uplus.batch.jobs.daily_agent_report;

import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
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
public class DailyAgentReportJobConfig {

  private final MongoTemplate mongoTemplate;
  private final DailyAgentReportProcessor dailyAgentReportProcessor;
  private final PlatformTransactionManager transactionManager;

  @Bean
  public Job dailyAgentReportJob(JobRepository jobRepository, Step dailyAgentReportStep) {
    return new JobBuilder("dailyAgentReportJob", jobRepository)
        .start(dailyAgentReportStep)
        .build();
  }

  @Bean
  public Step dailyAgentReportStep(JobRepository jobRepository) {
    return new StepBuilder("dailyAgentReportStep", jobRepository)
        .<Long, DailyAgentReportSnapshot>chunk(10, transactionManager) // 10명씩 처리
        .reader(agentIdReader())
        .processor(dailyAgentReportProcessor)
        .writer(mongoSnapshotWriter())
        .build();
  }

//  @Bean
//  @StepScope // 실행 시점에 생성되도록 설정
//  public ItemReader<String> agentIdReader() {
//    List<String> agentIds = mongoTemplate.getCollection("consultation_summary")
//        .distinct("agent._id", Integer.class)
//        .into(new ArrayList<>())
//        .stream().map(String::valueOf).collect(Collectors.toList());
//    return new ListItemReader<>(agentIds);
//  }
@Bean
@StepScope
public ItemReader<Long> agentIdReader() { // 1. 반환 타입을 Long으로 변경
  List<Long> agentIds = mongoTemplate.getCollection("consultation_summary")
      .distinct("agent._id", Long.class) // 2. DB에서 Long 타입으로 추출
      .into(new ArrayList<Long>())
      .stream()
      .collect(Collectors.toList()); // 3. String 변환 로직 삭제

  return new ListItemReader<>(agentIds);
}

  @Bean
  public ItemWriter<DailyAgentReportSnapshot> mongoSnapshotWriter() {
    return items -> items.forEach(mongoTemplate::save);
  }
}
