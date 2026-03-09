package com.uplus.batch.jobs.daily_agent_report;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uplus.batch.common.elasticsearch.ElasticsearchAnalyzeService;
import com.uplus.batch.domain.extraction.entity.ConsultationRawText;
import com.uplus.batch.domain.extraction.repository.ConsultationRawTextRepository;
import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot.QualityAnalysis;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyMetrics;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
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
 * consultation_raw_texts(MySQL)에서 원문 대화를 가져와 응대 품질 분석 수행.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@StepScope
public class DailyAgentReportProcessor implements ItemProcessor<Long, DailyAgentReportSnapshot> {

  private final MongoTemplate mongoTemplate;
  private final ConsultationRawTextRepository rawTextRepository;
  private final ObjectMapper objectMapper;
  private final ElasticsearchAnalyzeService esAnalyzeService;

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

    // [테스트용] 2025-01-18 데이터로 고정
    LocalDate targetDate = LocalDate.of(2025, 1, 18);

    // [운영용]
    // 배치는 어제 날짜 데이터를 집계합니다.
//    LocalDate targetDate = LocalDate.now().minusDays(1);

    // 1. 카테고리별 집계
    List<CategoryRanking> rankings = aggregateCategoryRanking(agentId, targetDate);

    // 2. 전체 성과 지표 집계 (평균 소요 시간, 만족도 포함)
    DailyMetrics metrics = aggregateDailyMetrics(agentId, targetDate);

    // 3. 응대 품질 분석
    QualityAnalysis qualityAnalysis = analyzeQuality(agentId, targetDate);

    // 4. 상담사 이름 조회
    String agentName = findAgentName(agentId, targetDate);

    // 5. 스냅샷 객체 생성
    return DailyAgentReportSnapshot.builder()
        .agentId(agentId)
        .agentName(agentName)
        .startAt(targetDate)
        .endAt(targetDate)
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

  // ==================== 응대 품질 분석 ====================

  /**
   * 상담사의 해당일 상담 원문을 분석하여 품질 메트릭을 계산한다.
   *
   * 1) MongoDB consultation_summary → consultId 목록 조회
   * 2) MySQL consultation_raw_texts → 원문 텍스트 일괄 조회
   * 3) ES _analyze API (korean_analysis_index_analyzer) → 동의어 사전 적용 토큰 반환
   * 4) 품질 카테고리별 토큰 카운트 → 비율/횟수 산출
   */
  private QualityAnalysis analyzeQuality(Long agentId, LocalDate date) {
    // 1. 해당 상담사의 consultId 목록 조회 (MongoDB)
    List<Long> consultIds = getConsultIds(agentId, date);
    if (consultIds.isEmpty()) {
      return null;
    }

    // 2. 원문 텍스트 일괄 조회 (MySQL)
    List<ConsultationRawText> rawTexts = rawTextRepository.findAllByConsultIdIn(consultIds);
    if (rawTexts.isEmpty()) {
      return null;
    }

    // 3. 각 상담별 ES 분석
    long empathyTotal = 0;
    int apologyCount = 0;
    int closingCount = 0;
    int courtesyCount = 0;
    int promptnessCount = 0;
    int accuracyCount = 0;
    int waitingGuideCount = 0;
    int analyzedCount = 0;

    for (ConsultationRawText rawText : rawTexts) {
      String agentText = extractAgentText(rawText.getRawTextJson());
      if (agentText.isEmpty()) {
        continue;
      }

      // ES _analyze API 호출 → 동의어 사전 적용된 토큰 리스트 반환
      List<String> tokens;
      try {
        tokens = esAnalyzeService.analyzeForQuality(agentText);
      } catch (IOException e) {
        log.warn("[Quality] ES 분석 실패 (consultId={}): {}", rawText.getConsultId(), e.getMessage());
        continue;
      }

      analyzedCount++;

      // 토큰에서 품질 카테고리별 카운트
      long empathy = tokens.stream().filter(TOKEN_EMPATHY::equals).count();
      empathyTotal += empathy;
      if (tokens.contains(TOKEN_APOLOGY)) apologyCount++;
      if (tokens.contains(TOKEN_CLOSING) || tokens.contains(TOKEN_THANKS)) closingCount++;
      if (tokens.contains(TOKEN_COURTESY)) courtesyCount++;
      if (tokens.contains(TOKEN_PROMPTNESS)) promptnessCount++;
      if (tokens.contains(TOKEN_ACCURACY)) accuracyCount++;
      if (tokens.contains(TOKEN_WAITING)) waitingGuideCount++;
    }

    if (analyzedCount == 0) {
      return null;
    }

    // 4. 비율 계산
    double apologyRate = round1((double) apologyCount / analyzedCount * 100);
    double closingRate = round1((double) closingCount / analyzedCount * 100);
    double courtesyRate = round1((double) courtesyCount / analyzedCount * 100);
    double promptnessRate = round1((double) promptnessCount / analyzedCount * 100);
    double accuracyRate = round1((double) accuracyCount / analyzedCount * 100);
    double waitingGuideRate = round1((double) waitingGuideCount / analyzedCount * 100);
    double avgEmpathy = round1((double) empathyTotal / analyzedCount);

    // 5. totalScore 계산 (5점 만점)
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
        agentId, analyzedCount, empathyTotal, apologyRate, closingRate, totalScore);

    return QualityAnalysis.builder()
        .analyzedCount(analyzedCount)
        .empathyCount(empathyTotal)
        .avgEmpathyPerConsult(avgEmpathy)
        .apologyRate(apologyRate)
        .closingRate(closingRate)
        .courtesyRate(courtesyRate)
        .promptnessRate(promptnessRate)
        .accuracyRate(accuracyRate)
        .waitingGuideRate(waitingGuideRate)
        .totalScore(totalScore)
        .build();
  }

  /**
   * MongoDB consultation_summary에서 해당 상담사의 consultId 목록 조회
   */
  private List<Long> getConsultIds(Long agentId, LocalDate date) {
    LocalDateTime startDt = date.atStartOfDay();
    LocalDateTime endDt = date.atTime(LocalTime.MAX);

    Query query = new Query(
        Criteria.where("agent._id").is(agentId)
            .and("consultedAt").gte(startDt).lte(endDt)
    );
    query.fields().include("consultId");

    return mongoTemplate.find(query, Document.class, "consultation_summary")
        .stream()
        .map(doc -> {
          Object val = doc.get("consultId");
          return val instanceof Number ? ((Number) val).longValue() : null;
        })
        .filter(id -> id != null)
        .collect(Collectors.toList());
  }

  /**
   * rawTextJson에서 상담사 발화만 추출하여 하나의 문자열로 합친다.
   */
  private String extractAgentText(String rawTextJson) {
    try {
      List<Map<String, String>> turns = objectMapper.readValue(
          rawTextJson, new TypeReference<>() {});

      return turns.stream()
          .filter(turn -> "상담사".equals(turn.get("speaker")))
          .map(turn -> turn.getOrDefault("text", ""))
          .collect(Collectors.joining(" "));
    } catch (Exception e) {
      log.warn("[Quality] rawTextJson 파싱 실패: {}", e.getMessage());
      return "";
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
            .and("consultedAt").gte(start).lte(end)),

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
