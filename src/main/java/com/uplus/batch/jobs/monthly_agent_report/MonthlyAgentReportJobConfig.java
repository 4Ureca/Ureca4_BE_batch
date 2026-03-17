package com.uplus.batch.jobs.monthly_agent_report;

import com.uplus.batch.jobs.monthly_agent_report.entity.MonthlyAgentReportSnapshot;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
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
        .reader(monthlyAgentIdReader(null, null))
        .processor(monthlyAgentReportProcessor)
        .writer(monthlySnapshotWriter())
        .build();
  }


  @Bean
  @StepScope
  public ItemReader<Long> monthlyAgentIdReader(
      @Value("#{jobParameters['startDate'] ?: null}") String startDateParam,
      @Value("#{jobParameters['endDate'] ?: null}") String endDateParam) {

    LocalDate startDate = (startDateParam != null && !startDateParam.isEmpty())
        ? LocalDate.parse(startDateParam)
        : LocalDate.now().minusMonths(1).withDayOfMonth(1);
    LocalDate endDate = (endDateParam != null && !endDateParam.isEmpty())
        ? LocalDate.parse(endDateParam)
        : LocalDate.now().minusMonths(1).with(TemporalAdjusters.lastDayOfMonth());

    LocalDateTime startAt = startDate.atStartOfDay();
    LocalDateTime endAt = endDate.atTime(23, 59, 59);

    // 해당 월에 상담 기록이 있는 상담사만 조회
    List<Long> agentIds = mongoTemplate.findDistinct(
        new Query(Criteria.where("consultedAt").gte(startAt).lte(endAt)),
        "agent._id",
        "consultation_summary",
        Long.class
    );

    return new ListItemReader<>(agentIds);
  }

  @Bean
  public ItemWriter<MonthlyAgentReportSnapshot> monthlySnapshotWriter() {
    // 월별 전용 컬렉션에 저장 (동일 agentId+startAt 존재 시 교체)
    return items -> items.forEach(item -> {
      Query query = new Query(
          Criteria.where("agentId").is(item.getAgentId())
              .and("startAt").is(item.getStartAt())
      );
      mongoTemplate.remove(query, "monthly_agent_report_snapshot");
      mongoTemplate.save(item, "monthly_agent_report_snapshot");
    });
  }
}