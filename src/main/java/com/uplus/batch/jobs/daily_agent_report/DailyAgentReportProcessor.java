package com.uplus.batch.jobs.daily_agent_report;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyMetrics;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@StepScope
public class DailyAgentReportProcessor implements ItemProcessor<Long, DailyAgentReportSnapshot> {

  private final MongoTemplate mongoTemplate;

  @Override
  public DailyAgentReportSnapshot process(Long agentId) {

    // [테스트용] 2025-01-15 데이터로 고정
    LocalDate targetDate = LocalDate.of(2025, 1, 15);

    // [운영용]
    // 배치는 어제 날짜 데이터를 집계합니다.
//    LocalDate targetDate = LocalDate.now().minusDays(1);

    // 1. 카테고리별 집계 (기존에 작성했던 Aggregation 로직 활용)
    List<CategoryRanking> rankings = aggregateCategoryRanking(agentId, targetDate);

    // 2. 전체 성과 지표 집계 (평균 소요 시간, 만족도 포함)
    DailyMetrics metrics = aggregateDailyMetrics(agentId, targetDate);

    // 3. 스냅샷 객체 생성
    return DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .startAt(targetDate)
        .endAt(targetDate)
        .consultCount(metrics.getCount())
        .avgDurationMinutes(metrics.getAvgDuration() / 60.0) // 초 단위를 분 단위로 변환
        .customerSatisfaction(metrics.getAvgSatisfaction() != null ? metrics.getAvgSatisfaction() : 0.0) // 고객 만족도 평균
        .categoryRanking(rankings)
        .build();
  }


  // 처리 카테고리별 건수 및 순위
  private List<CategoryRanking> aggregateCategoryRanking(Long agentId, LocalDate date) {
    LocalDateTime startDt = date.atStartOfDay();
    LocalDateTime endDt = date.atTime(LocalTime.MAX);

    MatchOperation match = Aggregation.match(
        Criteria.where("agent._id").is(agentId)
            .and("consultedAt").gte(startDt).lte(endDt)
    );


    GroupOperation group = Aggregation.group("category.code") // 중분류 코드로 그룹핑
        .first("category.large").as("large")            // 대분류 명칭
        .first("category.medium").as("medium")             // 중분류 명칭
        .count().as("count");                           // 해당 중분류의 인입 건수

    ProjectionOperation project = Aggregation.project("large", "medium", "count")
        .and("_id").as("code"); // 중분류 코드를 code 필드로 사용

    SortOperation sort = Aggregation.sort(Sort.Direction.DESC, "count");


    Aggregation aggregation = Aggregation.newAggregation(match, group, project, sort);

    List<CategoryRanking> results = mongoTemplate.aggregate(aggregation, "consultation_summary", CategoryRanking.class)
        .getMappedResults();

    for (int i = 0; i < results.size(); i++) {
      results.get(i).setRank(i + 1);
    }
    return results;
  }


  // 전체 상담 성과(처리건수, 소요시간, 고객만족도)
  private DailyMetrics aggregateDailyMetrics(Long agentId, LocalDate date) {
    LocalDateTime start = date.atStartOfDay();
    LocalDateTime end = date.atTime(LocalTime.MAX);

    Aggregation aggregation = Aggregation.newAggregation(
        Aggregation.match(Criteria.where("agent._id").is(agentId)
            .and("consultedAt").gte(start).lte(end)),

        Aggregation.group("agent._id")
            .count().as("count")
            .avg("durationSec").as("avgDuration")
            .avg("customer.satisfiedScore").as("avgSatisfaction")
    );

    AggregationResults<DailyMetrics> results = mongoTemplate.aggregate(
        aggregation, "consultation_summary", DailyMetrics.class
    );

    return results.getUniqueMappedResult() != null ?
        results.getUniqueMappedResult() : new DailyMetrics(0, 0.0, 0.0);
  }


}
