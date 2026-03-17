package com.uplus.batch.jobs.weekly_agent_report;

import com.uplus.batch.jobs.weekly_agent_report.entity.WeeklyAgentReportSnapshot;
import java.time.DayOfWeek;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
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
        .reader(weeklyAgentIdReader(null, null))
        .processor(weeklyAgentReportProcessor)
        .writer(weeklySnapshotWriter())
        .build();
  }

  @Bean
  @StepScope
  public ItemReader<Long> weeklyAgentIdReader(
      @Value("#{jobParameters['startDate'] ?: null}") String startDateParam,
      @Value("#{jobParameters['endDate'] ?: null}") String endDateParam) {

    LocalDate startDate = (startDateParam != null && !startDateParam.isEmpty())
        ? LocalDate.parse(startDateParam)
        : LocalDate.now().minusWeeks(1).with(DayOfWeek.MONDAY);
    LocalDate endDate = (endDateParam != null && !endDateParam.isEmpty())
        ? LocalDate.parse(endDateParam)
        : LocalDate.now().minusWeeks(1).with(DayOfWeek.SUNDAY);

    LocalDateTime startAt = startDate.atStartOfDay();
    LocalDateTime endAt = endDate.atTime(23, 59, 59);

    // 해당 주간에 상담 기록이 있는 상담사만 조회
    List<Long> agentIds = mongoTemplate.findDistinct(
        new Query(Criteria.where("consultedAt").gte(startAt).lte(endAt)),
        "agent._id",
        "consultation_summary",
        Long.class
    );

    return new ListItemReader<>(agentIds);
  }

  @Bean
  public ItemWriter<WeeklyAgentReportSnapshot> weeklySnapshotWriter() {
    // 주별 리포트 전용 컬렉션에 저장 (동일 agentId+startAt 존재 시 교체)
    return items -> items.forEach(item -> {
      Query query = new Query(
          Criteria.where("agentId").is(item.getAgentId())
              .and("startAt").is(item.getStartAt())
      );
      mongoTemplate.remove(query, "weekly_agent_report_snapshot");
      mongoTemplate.save(item, "weekly_agent_report_snapshot");
    });
  }
}