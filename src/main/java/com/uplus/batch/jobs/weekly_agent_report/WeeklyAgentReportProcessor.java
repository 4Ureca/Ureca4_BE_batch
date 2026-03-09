package com.uplus.batch.jobs.weekly_agent_report;

import com.uplus.batch.jobs.daily_agent_report.entity.CategoryRanking;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot;
import com.uplus.batch.jobs.daily_agent_report.entity.DailyAgentReportSnapshot.QualityAnalysis;
import com.uplus.batch.jobs.weekly_agent_report.entity.WeeklyAgentReportSnapshot;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.HashMap;
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
public class WeeklyAgentReportProcessor implements ItemProcessor<Long, WeeklyAgentReportSnapshot> {

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
  public WeeklyAgentReportSnapshot process(Long agentId) {
    // [테스트용] 지난주 월요일 ~ 일요일 고정 (2025-01-13 ~ 2025-01-19)
    LocalDate startAt = LocalDate.of(2025, 1, 13);
    LocalDate endAt = LocalDate.of(2025, 1, 19);

    // [운영용]] 자동 계산 로직:
    // LocalDate now = LocalDate.now();
    // LocalDate startAt = now.minusWeeks(1).with(DayOfWeek.MONDAY);
    // LocalDate endAt = now.minusWeeks(1).with(DayOfWeek.SUNDAY);

    // 1. 해당 기간의 일별 스냅샷들을 가져옴
    Query query = new Query(
        Criteria.where("agentId").is(agentId)
            .and("startAt").gte(startAt).lte(endAt)
    );
    List<DailyAgentReportSnapshot> dailySnapshots = mongoTemplate.find(query, DailyAgentReportSnapshot.class);

    if (dailySnapshots.isEmpty()) return null;

    // 2. 데이터 합산 (건수 합산 및 카테고리 랭킹 재집계)
    long totalConsultCount = 0;
    double totalDurationSum = 0; // (평균 시간 * 건수)의 합산
    double totalSatisfactionSum = 0; // (만족도 * 건수)의 합산
    Map<String, CategoryRanking> combinedRankings = new HashMap<>();

    for (DailyAgentReportSnapshot day : dailySnapshots) {
      long dayCount = day.getConsultCount();
      totalConsultCount += day.getConsultCount();

      // 가중치 계산을 위한 합산 (건수가 0인 날은 제외됨)
      totalDurationSum += (day.getAvgDurationMinutes() * dayCount);
      totalSatisfactionSum += (day.getCustomerSatisfaction() * dayCount);

      // 카테고리 랭킹 합산 로직
      for (CategoryRanking r : day.getCategoryRanking()) {
        CategoryRanking existing = combinedRankings.getOrDefault(r.getCode(),
            new CategoryRanking(r.getCode(), r.getLarge(), r.getMedium(), 0, 0));
        existing.setCount(existing.getCount() + r.getCount());
        combinedRankings.put(r.getCode(), existing);
      }
    }

    // 3. 최종 평균 지표 산출
    double weeklyAvgDuration = totalConsultCount > 0 ? totalDurationSum / totalConsultCount : 0;
    double weeklyAvgSatisfaction = totalConsultCount > 0 ? totalSatisfactionSum / totalConsultCount : 0;

    // 4. 카테고리 재정렬 및 순위 부여
    List<CategoryRanking> sortedRankings = combinedRankings.values().stream()
        .sorted(Comparator.comparingLong(CategoryRanking::getCount).reversed())
        .collect(Collectors.toList());

    for (int i = 0; i < sortedRankings.size(); i++) {
      sortedRankings.get(i).setRank(i + 1);
    }

    // 5. 응대 품질 합산 (일별 → 주별 가중 평균)
    WeeklyAgentReportSnapshot.QualityAnalysis weeklyQuality =
        aggregateQuality(dailySnapshots, totalConsultCount);

    // 6. 주별 결과 생성
    return WeeklyAgentReportSnapshot.builder()
        .agentId(agentId)
        .startAt(startAt)
        .endAt(endAt)
        .consultCount(totalConsultCount)
        .avgDurationMinutes(weeklyAvgDuration)
        .customerSatisfaction(weeklyAvgSatisfaction)
        .categoryRanking(sortedRankings)
        .qualityAnalysis(weeklyQuality)
        .build();
  }

  /**
   * 일별 QualityAnalysis를 합산하여 주별 품질 메트릭을 계산한다.
   * 비율 항목은 상담 건수 기준 가중 평균으로 산출.
   */
  private WeeklyAgentReportSnapshot.QualityAnalysis aggregateQuality(
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

    WeeklyAgentReportSnapshot.QualityAnalysis result = new WeeklyAgentReportSnapshot.QualityAnalysis();
    result.setEmpathyCount(totalEmpathy);
    result.setAvgEmpathyPerConsult(avgEmpathy);
    result.setApologyRate(apologyRate);
    result.setClosingRate(closingRate);
    result.setCourtesyRate(courtesyRate);
    result.setPromptnessRate(promptnessRate);
    result.setAccuracyRate(accuracyRate);
    result.setWaitingGuideRate(waitingGuideRate);
    result.setTotalScore(totalScore);

    log.info("[WeeklyQuality] agent={} — 공감 {}회, 사과 {}%, 총점 {}",
        withQuality.get(0).getAgentId(), totalEmpathy, apologyRate, totalScore);

    return result;
  }

  private double round1(double value) {
    return Math.round(value * 10.0) / 10.0;
  }
}
