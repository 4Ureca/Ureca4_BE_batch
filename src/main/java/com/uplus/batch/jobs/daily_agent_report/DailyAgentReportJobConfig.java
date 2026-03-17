package com.uplus.batch.jobs.daily_agent_report;

import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
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
        .reader(agentIdReader(null))
        .processor(dailyAgentReportProcessor)
        .writer(mongoSnapshotWriter())
        .build();
  }


  @Bean
  @StepScope
  public ItemReader<Long> agentIdReader(
      @Value("#{jobParameters['targetDate'] ?: null}") String targetDateParam) {

    LocalDate targetDate = (targetDateParam != null && !targetDateParam.isEmpty())
        ? LocalDate.parse(targetDateParam)
        : LocalDate.now().minusDays(1);

    LocalDateTime startAt = targetDate.atStartOfDay();
    LocalDateTime endAt = targetDate.atTime(23, 59, 59);

    // 해당 날짜에 상담 기록이 있는 상담사만 조회
    List<Long> agentIds = mongoTemplate.findDistinct(
        new Query(Criteria.where("consultedAt").gte(startAt).lte(endAt)),
        "agent._id",
        "consultation_summary",
        Long.class
    );

    return new ListItemReader<>(agentIds);
  }

  @Bean
  public ItemWriter<DailyAgentReportSnapshot> mongoSnapshotWriter() {
    return items -> items.forEach(snapshot -> {
      Query query = new Query(
          Criteria.where("agentId").is(snapshot.getAgentId())
              .and("startAt").is(snapshot.getStartAt())
      );
      // 동일 agentId + startAt 조합이 존재하면 교체, 없으면 삽입
      mongoTemplate.remove(query, DailyAgentReportSnapshot.class);
      mongoTemplate.save(snapshot);
    });
  }
}