package com.uplus.batch.jobs.monthly_agent_report;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot.QualityAnalysis;
import com.uplus.batch.jobs.monthly_agent_report.entity.MonthlyAgentReportSnapshot;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@StepScope
public class MonthlyAgentReportProcessor implements
    ItemProcessor<Long, MonthlyAgentReportSnapshot> {

  private final MongoTemplate mongoTemplate;

  // totalScore 가중치 (DailyAgentReportProcessor와 동일)
  private static final double W_EMPATHY = 0.20;
  private static final double W_APOLOGY = 0.15;
  private static final double W_CLOSING = 0.20;
  private static final double W_COURTESY = 0.10;
  private static final double W_PROMPTNESS = 0.10;
  private static final double W_ACCURACY = 0.10;
  private static final double W_WAITING = 0.15;

  @Override
  public MonthlyAgentReportSnapshot process(Long agentId) {
    // [테스트용] 2025년 1월 데이터 집계 (1일 ~ 31일)
    LocalDate startAt = LocalDate.of(2025, 1, 1);
    LocalDate endAt = LocalDate.of(2025, 1, 31);

    // 실운영 시 자동 계산:
    // LocalDate startAt = LocalDate.now().minusMonths(1).withDayOfMonth(1);
    // LocalDate endAt = LocalDate.now().minusMonths(1).with(TemporalAdjusters.lastDayOfMonth());

    // 1. 해당 월의 모든 일별 스냅샷 조회
    Query query = new Query(
        Criteria.where("agentId").is(agentId)
            .and("startAt").gte(startAt).lte(endAt)
    );
    List<DailyAgentReportSnapshot> dailySnapshots = mongoTemplate.find(query, DailyAgentReportSnapshot.class);

    if (dailySnapshots.isEmpty()) return null;

    // 2. 데이터 합산 (건수 및 카테고리 랭킹)
    long totalConsultCount = 0;
    double totalDurationSum = 0;
    double totalSatisfactionSum = 0;

    long totalSurveyTotalCount = 0;    // 월간 설문 요청 총합
    long totalSurveyResponseCount = 0; // 월간 설문 응답 총합

    double totalIamMatchRateSum = 0; // IAM 일치율 합산 변수

    Map<String, CategoryRanking> combinedRankings = new HashMap<>();

    for (DailyAgentReportSnapshot day : dailySnapshots) {
      long dayCount = day.getConsultCount();
      totalConsultCount += day.getConsultCount();

      // 가중치 계산을 위한 합산 (건수가 0인 날은 제외됨)
      totalDurationSum += (day.getAvgDurationMinutes() * dayCount);
      totalSatisfactionSum += (day.getCustomerSatisfactionAnalysis().getSatisfactionScore() * dayCount);

      // 일별 일치율 * 상담건수 합산
      totalIamMatchRateSum += (day.getIamKeywordMatchAnalysis() * dayCount);

      // 일별 설문 카운트 합산 (가중 평균용)
      if (day.getCustomerSatisfactionAnalysis() != null) {
        totalSurveyTotalCount += day.getCustomerSatisfactionAnalysis().getSurveyTotalCount();
        totalSurveyResponseCount += day.getCustomerSatisfactionAnalysis().getSurveyResponseCount();
      }

      // 처리 카테고리 순위 및 건수
      for (CategoryRanking r : day.getCategoryRanking()) {
        CategoryRanking existing = combinedRankings.getOrDefault(r.getCode(),
            new CategoryRanking(r.getCode(), r.getLarge(), r.getMedium(), 0, 0));
        existing.setCount(existing.getCount() + r.getCount());
        combinedRankings.put(r.getCode(), existing);
      }
    }

    // 3. 최종 월별 지표 산출
    double monthlyAvgDuration = totalConsultCount > 0 ? totalDurationSum / totalConsultCount : 0;
    double monthlyAvgSatisfaction = totalConsultCount > 0 ? totalSatisfactionSum / totalConsultCount : 0;

    // 월간 가중 평균 일치율 계산
    double monthlyIamMatchRate = totalConsultCount > 0 ? totalIamMatchRateSum / totalConsultCount : 0;

    // 월간 평균 응답률 계산
    double monthlyAvgResponseRate = totalSurveyTotalCount > 0
        ? (double) totalSurveyResponseCount / totalSurveyTotalCount * 100.0 : 0;

    // 4. 카테고리 재정렬 및 순위 부여
    List<CategoryRanking> sortedRankings = combinedRankings.values().stream()
        .sorted(Comparator.comparingLong(CategoryRanking::getCount).reversed())
        .collect(Collectors.toList());

    for (int i = 0; i < sortedRankings.size(); i++) {
      sortedRankings.get(i).setRank(i + 1);
    }

    // 5. 응대 품질 합산 (일별 → 월별 가중 평균)
    MonthlyAgentReportSnapshot.QualityAnalysis monthlyQuality =
        aggregateQuality(dailySnapshots, totalConsultCount);

    // 6. 월별 결과 생성
    return MonthlyAgentReportSnapshot.builder()
        .agentId(agentId)
        .agentName(dailySnapshots.get(0).getAgentName())
        .startAt(startAt)
        .endAt(endAt)
        .consultCount(totalConsultCount)
        .avgDurationMinutes(monthlyAvgDuration) // 주간 가중 평균 소요 시간
        .iamKeywordMatchAnalysis(monthlyIamMatchRate) // 최종 월간 일치율 반영
        .customerSatisfactionAnalysis(
            MonthlyAgentReportSnapshot.CustomerSatisfactionAnalysis.builder()
                .satisfactionScore(monthlyAvgSatisfaction)
                .responseRate(monthlyAvgResponseRate)
                .surveyTotalCount(totalSurveyTotalCount)
                .surveyResponseCount(totalSurveyResponseCount)
                .build()
        )
        .categoryRanking(sortedRankings)
        .qualityAnalysis(monthlyQuality)
        .build();
  }

  /**
   * 일별 QualityAnalysis를 합산하여 월별 품질 메트릭을 계산한다.
   * 비율 항목은 상담 건수 기준 가중 평균으로 산출.
   */
  private MonthlyAgentReportSnapshot.QualityAnalysis aggregateQuality(
      List<DailyAgentReportSnapshot> dailySnapshots, long totalConsultCount) {

    List<DailyAgentReportSnapshot> withQuality = dailySnapshots.stream()
        .filter(d -> d.getQualityAnalysis() != null)
        .collect(Collectors.toList());

    if (withQuality.isEmpty() || totalConsultCount == 0) {
      return null;
    }

    long totalEmpathy = 0;
    double weightedApology = 0, weightedClosing = 0, weightedCourtesy = 0;
    double weightedPromptness = 0, weightedAccuracy = 0, weightedWaiting = 0;
    long qualityConsultCount = 0;

    for (DailyAgentReportSnapshot day : withQuality) {
      QualityAnalysis q = day.getQualityAnalysis();
      long dayCount = q.getAnalyzedCount();
      qualityConsultCount += dayCount;

      totalEmpathy += q.getEmpathyCount();
      weightedApology += q.getApologyRate() * dayCount;
      weightedClosing += q.getClosingRate() * dayCount;
      weightedCourtesy += q.getCourtesyRate() * dayCount;
      weightedPromptness += q.getPromptnessRate() * dayCount;
      weightedAccuracy += q.getAccuracyRate() * dayCount;
      weightedWaiting += q.getWaitingGuideRate() * dayCount;
    }

    if (qualityConsultCount == 0) return null;

    double apologyRate = round1(weightedApology / qualityConsultCount);
    double closingRate = round1(weightedClosing / qualityConsultCount);
    double courtesyRate = round1(weightedCourtesy / qualityConsultCount);
    double promptnessRate = round1(weightedPromptness / qualityConsultCount);
    double accuracyRate = round1(weightedAccuracy / qualityConsultCount);
    double waitingGuideRate = round1(weightedWaiting / qualityConsultCount);
    double avgEmpathy = round1((double) totalEmpathy / qualityConsultCount);

    double empathyScore = Math.min(avgEmpathy / 3.0, 1.0);
    double totalScore = round1(
        (empathyScore * W_EMPATHY
            + apologyRate / 100.0 * W_APOLOGY
            + closingRate / 100.0 * W_CLOSING
            + courtesyRate / 100.0 * W_COURTESY
            + promptnessRate / 100.0 * W_PROMPTNESS
            + accuracyRate / 100.0 * W_ACCURACY
            + waitingGuideRate / 100.0 * W_WAITING) * 5.0);

    MonthlyAgentReportSnapshot.QualityAnalysis result = new MonthlyAgentReportSnapshot.QualityAnalysis();
    result.setAnalyzedCount(qualityConsultCount);
    result.setEmpathyCount(totalEmpathy);
    result.setAvgEmpathyPerConsult(avgEmpathy);
    result.setApologyRate(apologyRate);
    result.setClosingRate(closingRate);
    result.setCourtesyRate(courtesyRate);
    result.setPromptnessRate(promptnessRate);
    result.setAccuracyRate(accuracyRate);
    result.setWaitingGuideRate(waitingGuideRate);
    result.setTotalScore(totalScore);

    log.info("[MonthlyQuality] agent={} — 공감 {}회, 사과 {}%, 총점 {}",
        withQuality.get(0).getAgentId(), totalEmpathy, apologyRate, totalScore);

    return result;
  }

  private double round1(double value) {
    return Math.round(value * 10.0) / 10.0;
  }
}
