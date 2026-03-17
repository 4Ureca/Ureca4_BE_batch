package com.uplus.batch.jobs.daily_agent_report;

import com.uplus.batch.common.elasticsearch.QualityAnalysisAggregator;
import com.uplus.batch.common.elasticsearch.QualityAnalysisAggregator.QualityAggResult;
import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot.QualityAnalysis;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyMetrics;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

/**
 * 상담사 일별 리포트 Processor
 *
 * consultation_summary(MongoDB)에서 카테고리 집계,
 * consult-keyword-index(ES)에서 응대 품질 분석 수행.
 */
@Slf4j
@Component
@StepScope
public class DailyAgentReportProcessor implements ItemProcessor<Long, DailyAgentReportSnapshot> {

  private final MongoTemplate mongoTemplate;
  private final QualityAnalysisAggregator qualityAnalysisAggregator;
  private final String targetDateParam;

  public DailyAgentReportProcessor(
      MongoTemplate mongoTemplate,
      QualityAnalysisAggregator qualityAnalysisAggregator,
      @Value("#{jobParameters['targetDate'] ?: null}") String targetDateParam) {
    this.mongoTemplate = mongoTemplate;
    this.qualityAnalysisAggregator = qualityAnalysisAggregator;
    this.targetDateParam = targetDateParam;
  }

  // === ES analysis_synonyms.txt 동의어 매핑 결과 토큰 ===
  private static final String TOKEN_EMPATHY = "공감응대";
  private static final String TOKEN_APOLOGY = "사과표현";
  private static final String TOKEN_CLOSING = "마무리인사";
  private static final String TOKEN_THANKS = "감사인사";
  private static final String TOKEN_COURTESY = "친절응대";
  private static final String TOKEN_PROMPTNESS = "신속응대";
  private static final String TOKEN_ACCURACY = "정확응대";
  private static final String TOKEN_WAITING = "대기안내";

  // === totalScore 가중치 (합계 = 1.0, 결과 × 5.0 = 5점 만점) ===
  private static final double W_EMPATHY = 0.20;
  private static final double W_APOLOGY = 0.15;
  private static final double W_CLOSING = 0.20;
  private static final double W_COURTESY = 0.10;
  private static final double W_PROMPTNESS = 0.10;
  private static final double W_ACCURACY = 0.10;
  private static final double W_WAITING = 0.15;

  @Override
  public DailyAgentReportSnapshot process(Long agentId) {

    LocalDate targetDate = (targetDateParam != null && !targetDateParam.isEmpty())
        ? LocalDate.parse(targetDateParam)
        : LocalDate.now().minusDays(1);

    // 1. 전체 성과 지표 집계 (평균 소요 시간, 만족도 포함)
    DailyMetrics metrics = aggregateDailyMetrics(agentId, targetDate);
    if (metrics.getCount() == 0) {
      return null;
    }

    // 2. 카테고리별 집계
    List<CategoryRanking> rankings = aggregateCategoryRanking(agentId, targetDate);

    // 3. 응대 품질 분석 (ES consult-keyword-index aggregation)
    QualityAnalysis qualityAnalysis = analyzeQuality(agentId, targetDate);

    // 4. 상담사 이름 조회
    String agentName = findAgentName(agentId, targetDate);

    // 5. 스냅샷 객체 생성
    return DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .agentName(agentName)
        .startAt(targetDate.atStartOfDay())
        .endAt(targetDate.atTime(23, 59, 59))
        .consultCount(metrics.getCount())
        .avgDurationMinutes(metrics.getAvgDuration() / 60.0) // 초 단위를 분 단위로 변환
        .iamKeywordMatchAnalysis(metrics.getAvgIamMatchRate())
        .customerSatisfactionAnalysis(
            DailyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(metrics.getAvgSatisfaction() != null ? metrics.getAvgSatisfaction() : 0.0)
                // 응답률 계산: (응답건수 / 전체건수) * 100
                .responseRate(metrics.getCount() > 0
                    ? (double) metrics.getCompletedSurveyCount() / metrics.getCount() * 100.0 : 0)
                .surveyTotalCount((int) metrics.getCount())          // 주간/월간 집계용 1
                .surveyResponseCount((int) metrics.getCompletedSurveyCount()) // 주간/월간 집계용 2
                .build()
        )
        .categoryRanking(rankings)
        .qualityAnalysis(qualityAnalysis)
        .build();
  }

  // ==================== 응대 품질 분석 (ES Aggregation) ====================

  /**
   * consult-keyword-index에서 agent.quality 필드의 품질 토큰을 집계하여 품질 메트릭을 계산한다.
   * ES terms + filters aggregation으로 N건을 한 번의 쿼리로 처리.
   */
  private QualityAnalysis analyzeQuality(Long agentId, LocalDate date) {
    try {
      QualityAggResult result = qualityAnalysisAggregator.aggregate(agentId, date);

      if (result == null) {
        return null;
      }

      long total = result.totalDocs();

      // 비율 계산 (%)
      double apologyRate = round1((double) result.apologyDocCount() / total * 100);
      double closingRate = round1((double) result.closingDocCount() / total * 100);
      double courtesyRate = round1((double) result.courtesyDocCount() / total * 100);
      double promptnessRate = round1((double) result.promptnessDocCount() / total * 100);
      double accuracyRate = round1((double) result.accuracyDocCount() / total * 100);
      double waitingGuideRate = round1((double) result.waitingDocCount() / total * 100);
      double avgEmpathy = round1((double) result.empathyDocCount() / total);

      // totalScore 계산 (5점 만점)
      double empathyScore = Math.min(avgEmpathy / 3.0, 1.0); // 건당 3회 이상이면 만점
      double totalScore = round1(
          (empathyScore * W_EMPATHY
              + apologyRate / 100.0 * W_APOLOGY
              + closingRate / 100.0 * W_CLOSING
              + courtesyRate / 100.0 * W_COURTESY
              + promptnessRate / 100.0 * W_PROMPTNESS
              + accuracyRate / 100.0 * W_ACCURACY
              + waitingGuideRate / 100.0 * W_WAITING) * 5.0);

      log.info("[Quality] agent={} — 분석 {}건, 공감 {}회, 사과 {}%, 마무리 {}%, 총점 {}",
          agentId, total, result.empathyDocCount(), apologyRate, closingRate, totalScore);

      return QualityAnalysis.builder()
          .analyzedCount((int) total)
          .empathyCount(result.empathyDocCount())
          .avgEmpathyPerConsult(avgEmpathy)
          .apologyRate(apologyRate)
          .closingRate(closingRate)
          .courtesyRate(courtesyRate)
          .promptnessRate(promptnessRate)
          .accuracyRate(accuracyRate)
          .waitingGuideRate(waitingGuideRate)
          .totalScore(totalScore)
          .build();

    } catch (Exception e) {
      log.error("[Quality] ES 집계 실패 (agentId={}): {}", agentId, e.getMessage());
      return null;
    }
  }

  private double round1(double value) {
    return Math.round(value * 10.0) / 10.0;
  }

  // ==================== 상담사 이름 조회 ====================

  /**
   * consultation_summary에서 상담사 이름을 조회한다.
   */
  private String findAgentName(Long agentId, LocalDate date) {
    LocalDateTime startDt = date.atStartOfDay();
    LocalDateTime endDt = date.atTime(LocalTime.MAX);

    Query query = new Query(
        Criteria.where("agent._id").is(agentId)
            .and("consultedAt").gte(startDt).lte(endDt)
    );
    query.fields().include("agent.name");
    query.limit(1);

    Document doc = mongoTemplate.findOne(query, Document.class, "consultation_summary");
    if (doc != null) {
      Document agentDoc = doc.get("agent", Document.class);
      if (agentDoc != null) {
        return agentDoc.getString("name");
      }
    }
    return null;
  }

  // ==================== 카테고리 집계 ====================

  private List<CategoryRanking> aggregateCategoryRanking(Long agentId, LocalDate date) {
    LocalDateTime startDt = date.atStartOfDay();
    LocalDateTime endDt = date.atTime(LocalTime.MAX);

    MatchOperation match = Aggregation.match(
        Criteria.where("agent._id").is(agentId)
            .and("consultedAt").gte(startDt).lte(endDt)
            .and("source").ne("SYNTHETIC")  // 합성 데이터 리포트 왜곡 방지
    );


    GroupOperation group = Aggregation.group("category.code") // 중분류 코드로 그룹핑
        .first("category.large").as("large")            // 대분류 명칭
        .first("category.medium").as("medium")             // 중분류 명칭
        .count().as("count");                           // 해당 중분류의 건수

    ProjectionOperation project = Aggregation.project("large", "medium", "count")
        .and("_id").as("code"); // 대분류 코드를 code 필드로 사용

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
            .and("consultedAt").gte(start).lte(end)
            .and("source").ne("SYNTHETIC")),  // 합성 데이터 리포트 왜곡 방지

        Aggregation.group("agent._id")
            .count().as("count")
            .avg("durationSec").as("avgDuration")
            .avg("customer.satisfiedScore").as("avgSatisfaction")
            .avg("iam.matchRates").as("avgIamMatchRate") // 원본 DB 필드 -> DTO 필드
            .sum(ConditionalOperators.when(Criteria.where("customer.satisfiedScore").gt(0))
                .then(1).otherwise(0)).as("completedSurveyCount")
    );

    AggregationResults<DailyMetrics> results = mongoTemplate.aggregate(
        aggregation, "consultation_summary", DailyMetrics.class
    );

    DailyMetrics metrics = results.getUniqueMappedResult();

    if (metrics == null) {
      return new DailyMetrics(0L, 0.0, 0.0, 0L, 0.0, 0.0);
    }

    metrics.calculateResponseRate();

    return metrics;
  }


}